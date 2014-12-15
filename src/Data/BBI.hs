{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}

module Data.BBI where

import qualified Data.Map.Strict as M
import Control.Applicative ((<$>))
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import System.IO
import Data.Maybe
import Data.Conduit
import qualified Data.Conduit.List as CL
import Control.Monad.State
import Codec.Compression.Zlib (decompress)
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
    header <- fromJust <$> getBbiFileHeader h
    Right t <- getChromTreeAsList (_endian header) h (fromIntegral $ _chrTreeOffset header)
    Right rtree <- getRTreeHeader (_endian header) h (fromIntegral $ _fullIndexOffset header)
    return $ BbiFile h header (M.fromList t) rtree

closeBbiFile :: BbiFile -> IO ()
closeBbiFile = hClose . _filehandle

getBbiFileHeader :: Handle -> IO (Maybe BbiFileHeader)
getBbiFileHeader h = do
    ft <- getFileType h
    case ft of
        Just (t, e) -> do
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
            return . Just $ BbiFileHeader t e v zl ct fd fi fc df as ts ub r
        _ -> return Nothing

getFileType :: Handle -> IO (Maybe (FileType, Endianness))
getFileType h = do
    hSeek h AbsoluteSeek 0
    magic <- B.hGet h 4
    let magicBE = readInt32 BE magic
        magicLE = readInt32 LE magic
    return $ case () of
        _ | magicBE == bigBedMagic -> Just (BigBed, BE)
          | magicBE == bigWigMagic -> Just (BigWig, BE)
          | magicLE == bigBedMagic -> Just (BigBed, LE)
          | magicLE == bigWigMagic -> Just (BigWig, LE)
          | otherwise -> Nothing
  where
    bigWigMagic = 0x888FFC26
    bigBedMagic = 0x8789F2EB
{-# INLINE getFileType #-}

getChromTreeAsList :: Endianness -> Handle -> Integer -> IO (Either String [(B.ByteString, (Int, Int))])
getChromTreeAsList e h offset = do
    hSeek h AbsoluteSeek offset
    magic <- hReadInt32 e h
    if magic /= chromTreeMagic
       then return $ Left "incorrect chromsome tree magic"
       else do
           blockSize <- hReadInt32 e h
           keySize <- hReadInt32 e h
           valSize <- hReadInt32 e h
           itemCount <- hReadInt64 e h
           reserved <- hReadInt64 e h
           Right <$> traverseTree blockSize keySize valSize
  where
    traverseTree bs ks vs = go
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

query :: BbiFile -> (B.ByteString, Int, Int) -> Source IO (B.ByteString, Int, Int, B.ByteString)
query fl (chr, start, end)
    | isNothing chrId = return ()
    | otherwise = do
        let cid = fst . fromJust $ chrId
        when (overlap cid start end (_startChromIx $ _rtree fl) 
                                    (_startBase $ _rtree fl)
                                    (_endChromIx $ _rtree fl)
                                    (_endBase $ _rtree fl) )
            $ do 
                blks <- lift $ findOverlappingBlocks endi handle rTreeOffset cid start end
                queryBlocks endi handle blks cid start end
                    $= CL.map (\(_,a,b,c) -> (chr,a,b,c))
  where

    handle = _filehandle fl
    endi = _endian $ _header fl
    chrId = M.lookup chr $ _chromTree fl
    rTreeOffset = fromIntegral $ (_fullIndexOffset . _header) fl + 48
{-# INLINE query #-}

queryBlocks :: Endianness -> Handle -> [(Int, Int)] -> Int -> Int -> Int -> Source IO (Int, Int, Int, B.ByteString)
queryBlocks e h blks cid start end = forM_ blks $ \(offset, size) -> do
    bs <- lift $ do hSeek h AbsoluteSeek $ fromIntegral offset
                    BL.hGet h size
    (toBedRecords e . BL.toStrict . decompress) bs $= CL.filter f
  where
    f (chrid, st, ed, _) = chrid == cid && ed > start && st < end
{-# INLINE queryBlocks #-}

toBedRecords :: Monad m => Endianness -> B.ByteString -> Producer m (Int, Int, Int, B.ByteString)
toBedRecords e = go
  where
    go s | B.null s = return ()
         | otherwise = do
             let chr = readInt32 e . B.take 4 $ s
                 start = readInt32 e . B.take 4 . B.drop 4 $ s
                 end = readInt32 e . B.take 4 . B.drop 8 $ s
                 (rest, remain) = B.break (==0) . B.drop 12 $ s
             yield (chr, start, end, rest)
             go $ B.tail remain
{-# INLINE toBedRecords #-}

findOverlappingBlocks :: Endianness -> Handle -> Integer -> Int -> Int -> Int -> IO [(Int, Int)]
findOverlappingBlocks e h i cid start end = catMaybes <$> go i
  where
    go i' = do
        hSeek h AbsoluteSeek i'
        (isLeaf, n) <- readNode 
        if isLeaf
           then replicateM n readLeafItem
           else concat <$> replicateM n readNonLeafItem

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

    readNonLeafItem = do 
        stCIx <- hReadInt32 e h
        st <- hReadInt32 e h
        edCIx <- hReadInt32 e h
        ed <- hReadInt32 e h
        next <- hReadInt64 e h
        if overlap cid start end stCIx st edCIx ed
           then go $ fromIntegral next
           else return []
{-# INLINE findOverlappingBlocks #-}

streamBbi :: BbiFile -> Source IO (B.ByteString, Int, Int, B.ByteString)
streamBbi fl = mapM_ (query fl) allChroms
  where
    allChroms = map (\(chr, (_, size)) -> (chr, 0, size-1)) . M.toList . _chromTree $ fl
