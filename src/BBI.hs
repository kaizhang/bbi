{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
module BBI where

import qualified Data.HashMap.Strict as M
import Data.Binary.Strict.Get
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import System.IO
import Data.Maybe
import Data.Conduit
import qualified Data.Conduit.List as CL
import Control.Monad.State
import Codec.Compression.Zlib (decompress)

data FileType = BigBed
              | BigWig
    deriving (Show)

data Endianness = LE
                | BE
    deriving (Show)

data BbiFile = BbiFile
    { _filehandle :: Handle
    , _header :: !BbiFileHeader
    , _chromTree :: !(M.HashMap B.ByteString (Int, Int))
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
    header <- fmap fromJust $ getBbiFileHeader h
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

hReadBool :: Handle -> IO Bool
hReadBool h = do
    bs <- B.hGet h $ 1
    return . fromRight . fst . runGet (fmap (toEnum . fromIntegral) getWord8) $ bs

readInt64 :: Endianness -> B.ByteString -> Int
readInt64 LE = fromIntegral . fromRight . fst . runGet getWord64le
readInt64 BE = fromIntegral . fromRight . fst . runGet getWord64be
{-# INLINE readInt64 #-}

hReadInt64 :: Endianness -> Handle -> IO Int
hReadInt64 e h = fmap (readInt64 e) . B.hGet h $ 8
{-# INLINE hReadInt64 #-}

readInt32 :: Endianness -> B.ByteString -> Int
readInt32 LE = fromIntegral . fromRight . fst . runGet getWord32le
readInt32 BE = fromIntegral . fromRight . fst . runGet getWord32be
{-# INLINE readInt32 #-}

hReadInt32 :: Endianness -> Handle -> IO Int
hReadInt32 e h = fmap (readInt32 e) . B.hGet h $ 4
{-# INLINE hReadInt32 #-}

readInt16 :: Endianness -> B.ByteString -> Int
readInt16 LE = fromIntegral . fromRight . fst . runGet getWord16le
readInt16 BE = fromIntegral . fromRight . fst . runGet getWord16be
{-# INLINE readInt16 #-}

hReadInt16 :: Endianness -> Handle -> IO Int
hReadInt16 e h = fmap (readInt16 e) . B.hGet h $ 2
{-# INLINE hReadInt16 #-}

fromRight :: Either a b -> b
fromRight (Right x) = x
fromRight _ = error "Is Left"
{-# INLINE fromRight #-}

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
           fmap Right $ traverseTree blockSize keySize valSize
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

    readLeafItem n = do key <- fmap (B.filter (/=0)) $ B.hGet h n
                        chromId <- hReadInt32 e h
                        chromSize <- hReadInt32 e h
                        return (key, (chromId, chromSize))

    readNonLeafItem n = do key <- fmap (B.filter (/=0)) $ B.hGet h n
                           childOffset <- hReadInt64 e h
                           return (key, childOffset)

data RTree = Node !Int !Int !Int !Int ![RTree]
           | Leaf !Int !Int !Int !Int !Int !Int
    deriving (Show)

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

{-
readRTree :: Endianness -> Handle -> Integer -> IO RTree
readRTree e h i = do
    hSeek h AbsoluteSeek i
    checkResult <- checkMagic h rtmagic
    if isNothing checkResult
       then error "read RTree fail"
       else do
           let e = fromJust checkResult
           blockSize <- hReadInt32 e h
           itemCount <- hReadInt64 e h
           startChromIx <- hReadInt32 e h
           startBase <- hReadInt32 e h
           endChromIx <- hReadInt32 e h
           endBase <- hReadInt32 e h
           endFileOffset <- hReadInt64 e h
           itemsPerSlot <- hReadInt32 e h
           reserved <- hReadInt32 e h
           trees <- buildTree e
           return $ Node startChromIx startBase endChromIx endBase trees

  where
    buildTree e' = do pos <- hTell h
                      go pos
      where
        go i' = do hSeek h AbsoluteSeek i'
                   (isLeaf, n) <- readNode
                   if isLeaf
                      then replicateM n $ readLeafItem
                      else replicateM n $ readNonLeafItem 

        readNode = do isLeaf <- hReadBool h
                      _ <- hReadBool h
                      count <- hReadInt16 e' h
                      return (isLeaf, count)

        readLeafItem = do a <- hReadInt32 e' h
                          b <- hReadInt32 e' h
                          c <- hReadInt32 e' h
                          d <- hReadInt32 e' h
                          e <- hReadInt64 e' h
                          f <- hReadInt64 e' h
                          return $ Leaf a b c d e f

        readNonLeafItem = do a <- hReadInt32 e' h
                             b <- hReadInt32 e' h
                             c <- hReadInt32 e' h
                             d <- hReadInt32 e' h
                             e <- hReadInt64 e' h
                             subtrees <- go $ fromIntegral e
                             return $ Node a b c d subtrees

    rtmagic = 0x2468ACE0

-- | check 4 bytes magic
checkMagic :: Handle -> Int -> IO (Maybe Endianness)
checkMagic h m = do
    bs <- B.hGet h 4
    let magicLE = readInt32 LE bs
        magicBE = readInt32 BE bs
    case () of
        _ | magicLE == m -> return $ Just LE
          | magicBE == m -> return $ Just BE
          | otherwise -> return Nothing
          -}

overlap :: Int -> Int -> Int -> Int -> Int -> Int -> Int -> Bool
overlap qChr qStart qEnd rStartChr rStart rEndChr rEnd = 
    (qChr, qStart) < (rEndChr, rEnd) && (qChr, qEnd) > (rStartChr, rStart)
{-# INLINE overlap #-}

query :: BbiFile -> (B.ByteString, Int, Int) -> Source IO (B.ByteString, Int, Int, B.ByteString)
query fl (chr, start, end)
    | isNothing chrId = return ()
    | otherwise = do
        let cid = fst . fromJust $ chrId
        if overlap cid start end (_startChromIx $ _rtree fl) 
                                 (_startBase $ _rtree fl)
                                 (_endChromIx $ _rtree fl)
                                 (_endBase $ _rtree fl)
            then do blks <- lift $ findOverlappingBlocks endi handle rTreeOffset cid start end
                    queryBlocks endi handle blks cid start end
                        $= CL.map (\(_,a,b,c) -> (chr,a,b,c))
            else return ()
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
toBedRecords e bs = go bs
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
findOverlappingBlocks e h i cid start end = fmap catMaybes $ go i
  where
    go i' = do
        hSeek h AbsoluteSeek i'
        (isLeaf, n) <- readNode 
        if isLeaf
           then replicateM n readLeafItem
           else fmap concat $ replicateM n readNonLeafItem

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
        return $ if (overlap cid start end stCIx st edCIx ed)
                    then Just (offset, size)
                    else Nothing

    readNonLeafItem = do 
        stCIx <- hReadInt32 e h
        st <- hReadInt32 e h
        edCIx <- hReadInt32 e h
        ed <- hReadInt32 e h
        next <- hReadInt64 e h
        if (overlap cid start end stCIx st edCIx ed)
           then go $ fromIntegral next
           else return []
{-# INLINE findOverlappingBlocks #-}

streamBbi :: BbiFile -> Source IO (B.ByteString, Int, Int, B.ByteString)
streamBbi fl = mapM_ (query fl) allChroms
  where
    allChroms = map (\(chr, (_, size)) -> (chr, 0, size-1)) . M.toList . _chromTree $ fl
