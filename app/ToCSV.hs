module Main where

import qualified Data.ByteString.Lazy as ByteString
import qualified Data.Csv as CSV
import qualified DBase (parse, csvHeader, csvRecords)
import System.Environment (getArgs)

main :: IO ()
main = do
  [path] <- getArgs
  contents <- ByteString.readFile path
  let dbf = case DBase.parse contents of
              Left err -> error err
              Right parsed -> parsed
  ByteString.putStr (CSV.encode $ DBase.csvHeader dbf : DBase.csvRecords dbf)

