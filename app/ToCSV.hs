-- | Convert a DBF file to CSV

module Main where

import Control.Applicative ((<**>), optional)
import qualified Data.ByteString.Lazy as ByteString
import qualified Data.Csv as CSV
import qualified DBase (parse, csvHeader, csvRecords)
import Options.Applicative (Parser, metavar)
import qualified Options.Applicative as OptsAp

optionsParser :: Parser (Maybe String)
optionsParser =
   optional (OptsAp.argument OptsAp.str (metavar "DBF file" <> OptsAp.action "file"))
   <**> OptsAp.helper

main :: IO ()
main = do
  path <- OptsAp.execParser (OptsAp.info optionsParser $ OptsAp.progDesc "Convert a DBF file to CSV")
  contents <- maybe ByteString.getContents ByteString.readFile path
  let dbf = case DBase.parse contents of
              Left err -> error err
              Right parsed -> parsed
  ByteString.putStr (CSV.encode $ DBase.csvHeader dbf : DBase.csvRecords dbf)
