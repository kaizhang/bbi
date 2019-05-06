module Data.BBI.BigBed
    ( BBedFile
    , openBBedFile
    , closeBBedFile
    , query
    ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.Map as M
import Conduit

import Data.BBI
import Data.BBI.Utils

newtype BBedFile = BBedFile BbiFile

openBBedFile :: FilePath -> IO BBedFile
openBBedFile fl = do
    bbi <- openBbiFile fl
    case (_filetype . _header) bbi of
        BigBed -> return $ BBedFile bbi
        _ -> error "not a bigbed file"

closeBBedFile :: BBedFile -> IO ()
closeBBedFile (BBedFile fl) = closeBbiFile fl

-- | Find all regions that are overlapped with the query in the bigbed file.
query :: (B.ByteString, Int, Int)   -- ^ (Chr, start, end)
      -> BBedFile
      -> ConduitT () (B.ByteString, Int, Int, B.ByteString) IO ()
query (chr, s, e) (BBedFile fl) = case getChromId fl chr of
    Just i -> do
        blks <- lift $ overlappingBlocks fl (i, s, e)
        readBlocks fl blks .| concatMapC (map f . decodeBlock endi) .| filterC
          (\(_,s',e',_) -> isOverlapped (s',e') (s,e))
    Nothing -> return ()
  where
    endi = _endian $ _header fl
    id2Chr = M.fromList $ map (\(a,(b,_)) -> (b,a) ) $ M.toList $ _chromTree fl
    f (a,b,c,d) = (M.findWithDefault undefined a id2Chr, b, c, d)

-- | Whether two intervals are overlapped.
isOverlapped :: (Int, Int) -> (Int, Int) -> Bool
isOverlapped (lo1, hi1) (lo2, hi2) = not (lo2 > hi1 || lo1 > hi2)
{-# INLINE isOverlapped #-}

-- | Decode blocks in BigBed.
decodeBlock :: Endianness
            -> B.ByteString 
            -> [(Int, Int, Int, B.ByteString)]
decodeBlock e blk
    | B.null blk = []
    | otherwise = (chr, start, end, rest) : decodeBlock e (B.tail remain)
  where
    chr = readInt32 e $ B.take 4 blk
    start = readInt32 e $ B.take 4 $ B.drop 4 blk
    end = readInt32 e $ B.take 4 $ B.drop 8 blk
    (rest, remain) = BC.break (=='\0') $ B.drop 12 blk
{-# INLINE decodeBlock #-}
