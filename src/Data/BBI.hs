{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE LambdaCase #-}

module Data.BBI
    ( BbiFile(..)
    , BbiFileHeader(..)
    , FileType(..)
    , openBbiFile
    , closeBbiFile
    , getBbiFileHeader
    , getChromId
    , overlappingBlocks
    , readBlocks
    ) where

import Codec.Compression.Zlib (decompress)
import Control.Applicative ((<$>))
import Control.Monad.State
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Conduit
import Data.Maybe
import qualified Data.Map.Strict as M
import System.IO
import Data.BBI.Utils

data FileType = BigBed
              | BigWig
    deriving (Show)

-- | Get the file type.
getFileType :: Handle -> IO (FileType, Endianness)
getFileType h = do
    hSeek h AbsoluteSeek 0
    magic <- B.hGet h 4
    let magicBE = readInt32 BE magic
        magicLE = readInt32 LE magic
    return $ case () of
        _ | magicBE == bigBedMagic -> (BigBed, BE)
          | magicBE == bigWigMagic -> (BigWig, BE)
          | magicLE == bigBedMagic -> (BigBed, LE)
          | magicLE == bigWigMagic -> (BigWig, LE)
          | otherwise -> error "Not a bigBed or bigWig file!"
  where
    bigWigMagic = 0x888FFC26
    bigBedMagic = 0x8789F2EB
{-# INLINE getFileType #-}


-- | The header for big index file. Here are the Byte-by-byte details:
-- --------------------  ----  ----  -------------------------------------------
-- Name                  Size  Type	 Description
-- --------------------  ----  ----  -------------------------------------------
-- magic                 4     uint  0x888FFC26 for BigWig, 0x8789F2EB for
--                                   BigBed.  If byte-swapped, all
--                                   numbers in file are byte-swapped.
-- version               2     uint	 Currently 3. 
-- zoomLevels            2     uint	 Number of different zoom summary resolutions.
-- chromosomeTreeOffset	 8     uint	 Offset in file to chromosome B+ tree index.
-- fullDataOffset        8     uint	 Offset to main (unzoomed) data. Points
--                                   specifically to the dataCount.
-- fullIndexOffset       8     uint	 Offset to R tree index of items.
-- fieldCount            2     uint	 Number of fields in BED file. (0 for BigWig)
-- definedFieldCount     2     uint	 Number of fields that are predefined BED fields.
-- autoSqlOffset         8     uint	 Offset to zero-terminated string with .as spec.
-- totalSummaryOffset    8     uint	 Offset to overall file summary data block.
-- uncompressBufSize     4     uint	 Maximum size of decompression buffer
--                                   needed (nonzero on compressed files).
-- reserved              8     uint	 Reserved for future expansion. Currently 0.
data BbiFileHeader = BbiFileHeader
    { _filetype :: !FileType
    , _endian :: !Endianness
    , _version :: !Int
    , _zoomlevels :: !Int
    , _chrTreeOffset :: !Int
    , _fullDataOffset :: !Int
    , _fullIndexOffset :: !Int
    , _filedCount :: !Int
    , _definedFiledCount :: !Int
    , _autoSqlOffset :: !Int
    , _totalSummaryOffset :: !Int
    , _uncompressBufSize :: !Int
    , _reserved :: !Int
    } deriving (Show)

getBbiFileHeader :: Handle -> IO BbiFileHeader
getBbiFileHeader h = do
    (ft, endi) <- getFileType h 
    hSeek h AbsoluteSeek 4
    BbiFileHeader <$> return ft <*> return endi <*> hReadInt16 endi h <*>
        hReadInt16 endi h <*> hReadInt64 endi h <*> hReadInt64 endi h <*>
        hReadInt64 endi h <*> hReadInt16 endi h <*> hReadInt16 endi h <*>
        hReadInt64 endi h <*> hReadInt64 endi h <*> hReadInt32 endi h <*>
        hReadInt64 endi h
{-# INLINE getBbiFileHeader #-}


-- | R tree index header.
-- Name Size	Type	Description
-- magic	4	uint	0x2468ACE0.  If byte-swapped all numbers in index are byte-swapped.
-- blockSize	4	uint	Number of children per block (not byte size of block). 
-- itemCount	8	uint	The number of chromosomes/contigs.
-- startChromIx	4	uint	ID of first chromosome in index.
-- startBase	4	uint	Position of first base in index.
-- endChromIx	4	uint	ID of last chromosome in index.
-- endBase	4	uint	Position of last base in index.
-- endFileOffset	8	uint	Position in file where data being indexed ends.
-- itemsPerSlot	4	uint	Number of items pointed to by leaves of index.
-- Reserved	4	uint	Reserved for future expansion. Currently 0.
data RTreeHeader = RTreeHeader
    { _blockSize :: Int
    , _itemCount :: Int
    , _startChromIx :: Int
    , _startBase :: Int
    , _endChromIx :: Int
    , _endBase :: Int
    , _endFileOffset :: Int
    , _itemsPerSlot :: Int
    , _rTreeReserved :: Int
    } deriving (Show)

getRTreeHeader :: Endianness
               -> Handle
               -> Integer   -- ^ Location of R Tree header in the file.
               -> IO RTreeHeader
getRTreeHeader e h i = do
    magic <- hSeek h AbsoluteSeek i >> hReadInt32 e h
    if magic /= 0x2468ACE0
       then error "incorrect RTree magic"
       else RTreeHeader <$> hReadInt32 e h <*> hReadInt64 e h <*>
           hReadInt32 e h <*> hReadInt32 e h <*> hReadInt32 e h <*>
           hReadInt32 e h <*> hReadInt64 e h <*> hReadInt32 e h <*>
           hReadInt32 e h


-- | Big index file format. Here are the Byte-by-byte details:
-- --------------  ------  -----------------------------------------------------
-- Name            Size    Description
-- --------------  ------  -----------------------------------------------------
-- bbiHeader       64      Contains high-level information about file and
--                         offsets to various parts of file.
-- zoomHeaders     N*24    One for each level of zoom built into file.
-- autoSql         Varies  Zero-terminated string in autoSql format describing
--                         formats.  Optional, not used in BigWig.
-- totalSummary    40      Statistical summary of entire file.
-- chromosomeTree  Varies  B+ tree-formatted index of chromosomes, their sizes,
--                         and a unique ID for each.
-- dataCount       4       Number of records in data. For BigWig this
--                         corresponds to the number of sections, not the
--                         number of floating point values.
-- data            Varies  Possibly compressed data in format specific for
--                         file type.
-- index           Varies  R tree index of data.
-- zoomInfo        Varies  One for each zoom level.
data BbiFile = BbiFile
    { _filehandle :: Handle
    , _header :: !BbiFileHeader
    , _chromTree :: !(M.Map Chromosome (ChromID, ChromSize))
    , _rtree :: !RTreeHeader
    } deriving (Show)

type Chromosome = B.ByteString
type ChromSize = Int
type ChromID = Int

openBbiFile :: FilePath -> IO BbiFile
openBbiFile fl = do
    h <- openFile fl ReadMode
    header <- getBbiFileHeader h
    t <- getChromTreeAsList (_endian header) h (fromIntegral $ _chrTreeOffset header)
    rtree <- getRTreeHeader (_endian header) h (fromIntegral $ _fullIndexOffset header)
    return $ BbiFile h header (M.fromList t) rtree

closeBbiFile :: BbiFile -> IO ()
closeBbiFile = hClose . _filehandle

getChromTreeAsList :: Endianness
                   -> Handle
                   -> Integer   -- ^ Location
                   -> IO [(B.ByteString, (Int, Int))]
getChromTreeAsList e h offset = do
    magic <- hSeek h AbsoluteSeek offset >> hReadInt32 e h
    if magic /= 0x78CA8C91
       then error "wrong chromosome tree header"
       else do
           _ <- hReadInt32 e h -- blockSize, not used
           keySize <- hReadInt32 e h
           _ <- hReadInt32 e h -- valSize, not used
           _ <- hReadInt64 e h -- itemCount, not used
           _ <- hReadInt64 e h -- reserved, not used
           traverseTree keySize
  where
    traverseTree ks = go
      where
        go = readNode >>= \case
            (True, n) -> replicateM n $ readLeafItem ks
            (False, n) -> fmap concat $
                replicateM n (readNonLeafItem ks) >>= mapM ( \(_, off) -> do
                    hSeek h AbsoluteSeek $ fromIntegral off
                    go )
    readNode = do
        isLeaf <- hReadBool h
        _ <- hReadBool h
        count <- hReadInt16 e h
        return (isLeaf, count)
    readLeafItem n = do
        key <- B.filter (/=0) <$> B.hGet h n
        chromId <- hReadInt32 e h
        chromSize <- hReadInt32 e h
        return (key, (chromId, chromSize))
    readNonLeafItem n = do
        key <- B.filter (/=0) <$> B.hGet h n
        childOffset <- hReadInt64 e h
        return (key, childOffset)

overlap :: Int -> Int -> Int -> Int -> Int -> Int -> Int -> Bool
overlap qChr qStart qEnd rStartChr rStart rEndChr rEnd =
    (qChr, qStart) < (rEndChr, rEnd) && (qChr, qEnd) > (rStartChr, rStart)
{-# INLINE overlap #-}

getChromId :: BbiFile -> B.ByteString -> Maybe ChromID
getChromId fl chr = fmap fst . M.lookup chr . _chromTree $ fl
{-# INLINE getChromId #-}

overlappingBlocks :: BbiFile -> (ChromID, Int, Int) -> IO [Block]
overlappingBlocks fl (cid, start, end) =
    if overlap cid start end (_startChromIx $ _rtree fl)
                             (_startBase $ _rtree fl)
                             (_endChromIx $ _rtree fl)
                             (_endBase $ _rtree fl)
       then catMaybes <$> go ((fromIntegral . _fullIndexOffset . _header) fl + 48)
       else return []
  where
    h = _filehandle fl
    e = _endian . _header $ fl
    go i = do
        hSeek h AbsoluteSeek i
        (isLeaf, n) <- readNode
        if isLeaf
           then replicateM n readLeafItem
           else do
               pos <- fromIntegral <$> hTell h
               results <- mapM (readNonLeafItem . (\x -> pos + x*24)) [0..n-1]
               return $ concat results

    readNode = do isLeaf <- hReadBool h
                  _ <- hReadBool h
                  count <- hReadInt16 e h
                  return (isLeaf, count)

    readLeafItem = do
        stCIx <- hReadInt32 e h
        st <- hReadInt32 e h
        edCIx <- hReadInt32 e h
        ed <- hReadInt32 e h
        offset <- hReadInt64 e h
        size <- hReadInt64 e h
        return $ if overlap cid start end stCIx st edCIx ed
                    then Just (offset, size)
                    else Nothing

    readNonLeafItem p = do
        hSeek h AbsoluteSeek $ fromIntegral p
        stCIx <- hReadInt32 e h
        st <- hReadInt32 e h
        edCIx <- hReadInt32 e h
        ed <- hReadInt32 e h
        next <- hReadInt64 e h
        if overlap cid start end stCIx st edCIx ed
           then go $ fromIntegral next
           else return []
{-# INLINE overlappingBlocks #-}

type Block = (Offset, BlockSize)
type Offset = Int
type BlockSize = Int

-- | Read Blocks from the file. 
readBlocks :: BbiFile -> [Block] -> Source IO B.ByteString
readBlocks fl blks = forM_ blks $ \(offset, size) -> do
    bs <- lift $ hSeek handle AbsoluteSeek (fromIntegral offset) >>
        BL.hGet handle size
    yield . BL.toStrict . decompress $ bs
  where
    handle = _filehandle fl
{-# INLINE readBlocks #-}

{-
streamBbi :: BbiFile -> Source IO (B.ByteString, Int, Int, B.ByteString)
streamBbi fl = mapM_ (query fl) allChroms
  where
    allChroms = map (\(chr, (_, size)) -> (chr, 0, size-1)) . M.toList . _chromTree $ fl
    -}
