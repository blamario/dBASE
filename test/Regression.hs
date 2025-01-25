{-# LANGUAGE Haskell2010 #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString.Lazy qualified as Lazy
import Data.Csv qualified as CSV
import Data.Text.Lazy (toStrict)
import Data.Text.Lazy.Encoding (decodeUtf8)
import System.FilePath.Posix (combine, replaceExtension, takeFileName)

import Test.Tasty
import Test.Tasty.Silver

import DBase qualified

main :: IO ()
main = findByExtension [".dbf"] inputDir >>= defaultMain . testGroup "Regression" . map testFile

rootDir, inputDir, referenceDir :: FilePath
rootDir = combine "test" "regression"
inputDir = combine rootDir "inputs"
referenceDir = combine rootDir "reference"

testFile :: FilePath -> TestTree
testFile path =
  goldenVsAction baseName (combine referenceDir $ replaceExtension baseName "csv") (toCSV path) (toStrict . decodeUtf8)
  where baseName = takeFileName path

toCSV :: FilePath -> IO Lazy.ByteString
toCSV path = do
  contents <- Lazy.readFile path
  let dbf = case DBase.parse contents of
              Left err -> error err
              Right parsed -> parsed
  pure (CSV.encode $ DBase.csvHeader dbf : DBase.csvRecords dbf)
