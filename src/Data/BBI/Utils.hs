module Data.BBI.Utils
    ( Endianness(..)
    , hReadBool
    , readInt64
    , hReadInt64
    , readInt32
    , hReadInt32
    , readInt16
    , hReadInt16
    , readInt8
    , hReadInt8
    , readFloat32
    ) where

import qualified Data.ByteString as B
import Data.Binary.Strict.Get
import System.IO

data Endianness = LE
                | BE
    deriving (Show)

hReadBool :: Handle -> IO Bool
hReadBool h = do
    bs <- B.hGet h 1
    return . fromRight . fst . runGet (fmap (toEnum . fromIntegral) getWord8) $ bs
{-# INLINE hReadBool #-}

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

hReadInt8 :: Handle -> IO Int
hReadInt8 h = fmap readInt8 . B.hGet h $ 1
{-# INLINE hReadInt8 #-}

readInt8 :: B.ByteString -> Int
readInt8 = fromIntegral . fromRight . fst . runGet getWord8
{-# INLINE readInt8 #-}

readFloat32 :: B.ByteString -> Float
readFloat32 = fromRight . fst . runGet getFloat32host
{-# INLINE readFloat32 #-}

fromRight :: Either String b -> b
fromRight (Right x) = x
fromRight (Left s) = error s
{-# INLINE fromRight #-}
