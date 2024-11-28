module Main where

import qualified Data.ByteString.Lazy as ByteString
import qualified Data.Csv as CSV
import qualified DBase (parse, csvHeader, csvRecords)
import System.Environment (getArgs)

main :: IO ()
main = do
  [path] <- getArgs
  contents <- ByteString.readFile path
  let Just dbf = DBase.parse contents
  ByteString.putStr (CSV.encode $ DBase.csvHeader dbf : DBase.csvRecords dbf)

