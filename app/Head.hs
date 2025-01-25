{-# LANGUAGE Haskell2010, NamedFieldPuns #-}
module Main where

import Control.Applicative ((<|>), (<**>), optional)
import Data.Functor.Identity (Identity(Identity))
import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Lazy as ByteString
import DBase (DBaseFile(..), FileHeader(recordCount))
import qualified DBase (parse, serialize)
import Options.Applicative (Parser, long, metavar, short)
import qualified Options.Applicative as OptsAp


optionsParser :: Parser (Int, Maybe FilePath)
optionsParser =
   (,)
   <$> (OptsAp.option OptsAp.auto (short 'n' <> long "records"
                                   <> metavar "how many first records to keep, 10 by default")
        <|> pure 10)
   <*> optional (OptsAp.argument OptsAp.str (metavar "DBF file" <> OptsAp.action "file"))
   <**> OptsAp.helper

main :: IO ()
main = do
  (keep, path) <- OptsAp.execParser $ OptsAp.info optionsParser $ OptsAp.progDesc "keep only the first records in a DBF file"
  contents <- maybe ByteString.getContents ByteString.readFile path
  let dbf@DBaseFile{header, records} = case DBase.parse contents of
        Left err -> error err
        Right parsed -> parsed
  ByteString.putStr $ fromMaybe (error "can't re-serialize")
    $ DBase.serialize dbf{header = header{recordCount = min (Identity $ fromIntegral keep) $ recordCount header},
                          records = take keep records}
