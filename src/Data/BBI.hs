{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}

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
import Data.Conduit
import Data.Maybe
import qualified Data.Map.Strict as M
import System.IO
import Data.BBI.Utils

data FileType = BigBed
              | BigWig
    deriving (Show)

data BbiFile = BbiFile
    { _filehandle :: Handle
    , _header :: !BbiFileHeader
    , _chromTree :: !(M.Map B.ByteString (Int, Int))
    , _rtree :: !RTreeHeader
    } deriving (Show)

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

openBbiFile :: FilePath -> IO BbiFile
openBbiFile fl = do
    h <- openFile fl ReadMode
    header <- fromRight <$> getBbiFileHeader h
    t <- fromRight <$> getChromTreeAsList (_endian header) h (fromIntegral $ _chrTreeOffset header)
    rtree <- fromRight <$> getRTreeHeader (_endian header) h (fromIntegral $ _fullIndexOffset header)
    return $ BbiFile h header (M.fromList t) rtree

closeBbiFile :: BbiFile -> IO ()
closeBbiFile = hClose . _filehandle

getBbiFileHeader :: Handle -> IO (Either String BbiFileHeader)
getBbiFileHeader h = do
    ft <- getFileType h
    case ft of
        Right (t, e) -> do
            hSeek h AbsoluteSeek 4
            v <- hReadInt16 e h
            zl <- hReadInt16 e h
            ct <- hReadInt64 e h
            fd <- hReadInt64 e h
            fi <- hReadInt64 e h
            fc <- hReadInt16 e h
            df <- hReadInt16 e h
            as <- hReadInt64 e h
            ts <- hReadInt64 e h
            ub <- hReadInt32 e h
            r <- hReadInt64 e h
            return . Right $ BbiFileHeader t e v zl ct fd fi fc df as ts ub r
        Left e -> return $ Left e

getFileType :: Handle -> IO (Either String (FileType, Endianness))
getFileType h = do
    hSeek h AbsoluteSeek 0
    magic <- B.hGet h 4
    let magicBE = readInt32 BE magic
        magicLE = readInt32 LE magic
    return $ case () of
        _ | magicBE == bigBedMagic -> Right (BigBed, BE)
          | magicBE == bigWigMagic -> Right (BigWig, BE)
          | magicLE == bigBedMagic -> Right (BigBed, LE)
          | magicLE == bigWigMagic -> Right (BigWig, LE)
          | otherwise -> Left "not a bigBed/bigWig file"
  where
    bigWigMagic = 0x888FFC26
    bigBedMagic = 0x8789F2EB
{-# INLINE getFileType #-}

getChromTreeAsList :: Endianness -> Handle -> Integer -> IO (Either String [(B.ByteString, (Int, Int))])
getChromTreeAsList e h offset = do
    hSeek h AbsoluteSeek offset
    magic <- hReadInt32 e h
    if magic /= chromTreeMagic
       then return $ Left "wrong chromosome tree header"
       else do
           _ <- hReadInt32 e h -- blockSize, not used
           keySize <- hReadInt32 e h
           _ <- hReadInt32 e h -- valSize, not used
           _ <- hReadInt64 e h -- itemCount, not used
           _ <- hReadInt64 e h -- reserved, not used
           Right <$> traverseTree keySize
  where
    traverseTree ks = go
      where
        go = do (isLeaf, n) <- readNode
                if isLeaf
                   then replicateM n $ readLeafItem ks
                   else do
                       xs <- replicateM n $ readNonLeafItem ks
                       rs <- forM xs $ \(_, off) -> do
                           hSeek h AbsoluteSeek $ fromIntegral off
                           go
                       return $ concat rs

    chromTreeMagic = 0x78CA8C91

    readNode = do isLeaf <- hReadBool h
                  _ <- hReadBool h
                  count <- hReadInt16 e h
                  return (isLeaf, count)

    readLeafItem n = do key <- B.filter (/=0) <$> B.hGet h n
                        chromId <- hReadInt32 e h
                        chromSize <- hReadInt32 e h
                        return (key, (chromId, chromSize))

    readNonLeafItem n = do key <- B.filter (/=0) <$> B.hGet h n
                           childOffset <- hReadInt64 e h
                           return (key, childOffset)

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

getRTreeHeader :: Endianness -> Handle -> Integer -> IO (Either String RTreeHeader)
getRTreeHeader e h i = do
    hSeek h AbsoluteSeek i
    magic <- hReadInt32 e h
    if magic /= rtmagic
       then return $ Left "incorrect RTree magic"
       else do
           bs <- hReadInt32 e h
           ic <- hReadInt64 e h
           sc <- hReadInt32 e h
           sb <- hReadInt32 e h
           ec <- hReadInt32 e h
           eb <- hReadInt32 e h
           eo <- hReadInt64 e h
           ips <- hReadInt32 e h
           r <- hReadInt32 e h
           return $ Right $ RTreeHeader bs ic sc sb ec eb eo ips r
  where
    rtmagic = 0x2468ACE0

overlap :: Int -> Int -> Int -> Int -> Int -> Int -> Int -> Bool
overlap qChr qStart qEnd rStartChr rStart rEndChr rEnd = 
    (qChr, qStart) < (rEndChr, rEnd) && (qChr, qEnd) > (rStartChr, rStart)
{-# INLINE overlap #-}

getChromId :: BbiFile -> B.ByteString -> Maybe Int
getChromId fl chr = fmap fst . M.lookup chr . _chromTree $ fl
{-# INLINE getChromId #-}

overlappingBlocks :: BbiFile -> (Int, Int, Int) -> IO [(Int, Int)]
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

readBlocks :: BbiFile -> [(Int, Int)] -> Source IO B.ByteString
readBlocks fl blks = forM_ blks $ \(offset, size) -> do
    bs <- lift $ do hSeek handle AbsoluteSeek $ fromIntegral offset
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
