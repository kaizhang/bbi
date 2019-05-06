module Data.BBI.BigWig
    ( BWFile
    , openBWFile
    , closeBWFile
    , queryBWFile
    ) where

import Control.Monad (unless)
import Control.Monad.Trans (lift)
import qualified Data.ByteString as B
import Conduit
import Data.Maybe
import Data.BBI
import Data.BBI.Utils

newtype BWFile = BWFile BbiFile

openBWFile :: FilePath -> IO BWFile
openBWFile fl = do bbi <- openBbiFile fl
                   case (_filetype . _header) bbi of
                       BigWig -> return $ BWFile bbi
                       _ -> error "not a bigWig file"

closeBWFile :: BWFile -> IO ()
closeBWFile (BWFile fl) = closeBbiFile fl

-- | wig section type
data SectionType = VarStep    -- 0-indexed in binary
                 | FixedStep  -- 0-indexed in binary
                 | BedGraph   -- 0-indexed
    deriving (Show)

data WigHeader = WigHeader
    { _chromId :: !Int
    , _chromStart :: !Int
    , _chromEnd :: !Int
    , _itemStep :: !Int
    , _itemSpan :: !Int
    , _type :: !SectionType
    , _reserve :: !Int
    , _itemCount :: !Int
    } deriving (Show)

-- | the query should be 0-indexed
queryBWFile :: BWFile -> (B.ByteString, Int, Int) -> Source IO (B.ByteString, Int, Int, Double)
queryBWFile (BWFile fl) (chr, start, end) = unless (isNothing cid) $ do
    blks <- lift $ overlappingBlocks fl (fromJust cid, start, end)
    readBlocks fl blks $= toWigRecords endi $= filter'
  where
    filter' = concatMapC $ \(c, s, e, v) ->
                 let s' = max start s
                     e' = min end e
                 in if c == fromJust cid && s' < e'
                       then Just (chr, s', e', realToFrac v)
                       else Nothing
    cid = getChromId fl chr
    endi = _endian . _header $ fl
{-# INLINE queryBWFile #-}

-- | convert bytestring to wig style record, but the output is 0-indexed
toWigRecords :: Monad m => Endianness -> Conduit B.ByteString m (Int, Int, Int, Float)
toWigRecords endi = concatMapC $ \bs ->
    let header = readWigHeader endi bs
    in case _type header of
        VarStep -> readVarStep header $ B.drop 24 bs
        FixedStep -> readFixedStep header $ B.drop 24 bs
        BedGraph -> readBedGraph header $ B.drop 24 bs
  where
    readVarStep h = loop 0
      where
        loop i x | i >= n = []
                 | otherwise = let s = (readInt32 endi . B.take 4) x
                                   e = s + sp
                                   v = readFloat32 endi . B.take 4 . B.drop 4 $ x
                               in (cid, s, e, v) : loop (i+1) (B.drop 8 x)
        n = _itemCount h
        sp = _itemSpan h
        cid = _chromId h

    readFixedStep h = loop start 0
      where
        loop st i x | i >= n = []
                    | otherwise = let v = readFloat32 endi . B.take 4 $ x
                                  in (cid, st, st+sp, v) : loop (st+step) (i+1) (B.drop 4 x)
        n = _itemCount h
        sp = _itemSpan h
        step = _itemStep h
        start = _chromStart h
        cid = _chromId h

    readBedGraph h = loop 0
      where
        loop i x | i >= n = []
                 | otherwise = let s = readInt32 endi . B.take 4 $ x
                                   e = readInt32 endi . B.take 4 . B.drop 4 $ x
                                   v = readFloat32 endi . B.take 4 . B.drop 8 $ x
                               in (cid, s, e, v) : loop (i+1) (B.drop 12 x)
        n = _itemCount h
        cid = _chromId h
{-# INLINE toWigRecords #-}

readWigHeader :: Endianness -> B.ByteString -> WigHeader
readWigHeader e s = WigHeader (readInt32 e . B.take 4 $ s)
                              (readInt32 e . B.take 4 . B.drop 4 $ s)
                              (readInt32 e . B.take 4 . B.drop 8 $ s)
                              (readInt32 e . B.take 4 . B.drop 12 $ s)
                              (readInt32 e . B.take 4 . B.drop 16 $ s)
                              (f . readInt8 . B.take 1 . B.drop 20 $ s)
                              (readInt8 . B.take 1 . B.drop 21 $ s)
                              (readInt16 e . B.take 2 . B.drop 22 $ s)
  where
    f x | x == 1 = BedGraph
        | x == 2 = VarStep
        | x == 3 = FixedStep
        | otherwise = error "Unknown Wig seciton type"
{-# INLINE readWigHeader #-}
