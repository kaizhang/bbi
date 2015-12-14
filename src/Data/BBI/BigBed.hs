module Data.BBI.BigBed
    ( BBedFile
    , openBBedFile
    , closeBBedFile
    , toBedRecords
    ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Conduit
import qualified Data.Conduit.List as CL
import Data.BBI
import Data.BBI.Utils

newtype BBedFile = BBedFile BbiFile

openBBedFile :: FilePath -> IO BWFile
openBBedFile fl = do
    bbi <- openBbiFile fl
    case (_filetype . _header) bbi of
        BigBed -> return $ BBedFile bbi
        _ -> error "not a bigbed file"

closeBBedFile :: BBedFile -> IO ()
closeBBedFile (BBedFile fl) = closeBbiFile fl


-- | convert each block to BED records
toBedRecords :: Monad m
             => Endianness
             -> Conduit B.ByteString m (Int, Int, Int, B.ByteString)
toBedRecords e = concatMapC (map f . B.split '\0')
  where
    f s = (chr, start, end, rest)
      where
        chr = readInt32 e . B.take 4 $ s
        start = readInt32 e . B.take 4 . B.drop 4 $ s
        end = readInt32 e . B.take 4 . B.drop 8 $ s
        rest = B.drop 12 $ s
{-# INLINE toBedRecords #-}
