-- | Convert a CSV file to DBF
module Main where

import Control.Applicative ((<**>), optional)
import qualified Data.ByteString.Lazy as ByteString
import qualified Data.Csv as CSV
import Data.Foldable (toList)
import Data.Time.Clock (getCurrentTime, utctDay)
import DBase (DBaseFile(DBaseFile), headerFromCsv, recordFromCSV, serialize)
import Options.Applicative (Parser, metavar)
import qualified Options.Applicative as OptsAp

optionsParser :: Parser (Maybe String)
optionsParser =
   optional (OptsAp.argument OptsAp.str (metavar "CSV file" <> OptsAp.action "file"))
   <**> OptsAp.helper

main :: IO ()
main = do
  path <- OptsAp.execParser (OptsAp.info optionsParser $ OptsAp.progDesc "Convert a CSV file to DBF")
  contents <- maybe ByteString.getContents ByteString.readFile path
  now <- getCurrentTime
  let dbf = do
        (csvHeader, _) <- CSV.decodeByNameWithP (error "not parsing records yet") CSV.defaultDecodeOptions
          $ ByteString.takeWhile (/= 0xA) contents <> ByteString.singleton 0xA
        csvRecords <- CSV.decodeWithP pure CSV.defaultDecodeOptions CSV.HasHeader contents
        dbfHeader <- headerFromCsv csvHeader (utctDay now) csvRecords
        dbfRecords <- traverse (recordFromCSV dbfHeader) csvRecords
        maybe (Left $ "Cannot serialize DBF:\n" ++ show (dbfHeader, take 10 $ toList dbfRecords)) Right
          $ serialize (DBaseFile dbfHeader $ toList dbfRecords)
  case dbf of
    Left err -> error err
    Right bytes -> ByteString.putStr bytes
