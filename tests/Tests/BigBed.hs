{-# LANGUAGE OverloadedStrings #-}

module Tests.BigBed
    ( tests ) where

import Bio.Data.Bed
import Bio.Data.Bed.Types
import Conduit

import Test.Tasty
import Test.Tasty.HUnit

import Data.BBI.BigBed

tests :: TestTree
tests = testGroup "Test: BigBed"
    [ testCase "intersect" intersectTest
    ]

intersectTest :: Assertion
intersectTest = do
    bed <- readBed "tests/data/sample.bed"
    bbed <- openBBedFile "tests/data/sample.bigbed"
    compareIntersection (BED3 "chr1" 1 10000000) bed bbed

compareIntersection :: BED3 -> [BED3] -> BBedFile -> Assertion
compareIntersection x@(BED3 chr s e) bed bbed = do
    observed <- runConduit $ query (chr, s, e) bbed .| mapC f .| sinkList
    fromSorted (sortBed observed) @?= fromSorted (sortBed expected)
  where
    expected = runIdentity $ runConduit $
        yieldMany bed .| intersectBed [x] .| sinkList
    f (a,b,c,_) = BED3 a b c
