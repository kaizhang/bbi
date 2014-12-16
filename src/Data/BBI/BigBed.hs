module Data.BBI.BigBed where

import qualified Data.ByteString as B
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.BBI
import Data.BBI.Utils

toBedRecords :: Monad m => Endianness -> Conduit B.ByteString m (Int, Int, Int, B.ByteString)
toBedRecords e = do
    d <- await
    case d of
        Nothing -> return ()
        Just bs -> go bs
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
