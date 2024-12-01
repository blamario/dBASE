{-# LANGUAGE Haskell2010, TupleSections #-}
module Main where

import Control.Applicative ((<|>), (<**>), optional)
import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Lazy as ByteString
import qualified Data.ByteString.Char8 as ASCII
import qualified DBase (parse, project, serialize)
import Options.Applicative (Parser, long, metavar, short)
import qualified Options.Applicative as OptsAp


optionsParser :: Parser (Bool, String, Maybe String)
optionsParser =
   ((,,) True <$> OptsAp.strOption (short 'c' <> long "columns" <> metavar "comma-separated columns to keep")
    <|> (,,) False <$> OptsAp.strOption (short 'C' <> long "not-columns" <> metavar "comma-separated columns to delete"))
   <*> optional (OptsAp.argument OptsAp.str (metavar "DBF file" <> OptsAp.action "file"))
   <**> OptsAp.helper

main :: IO ()
main = do
  (keep, cols, path) <- OptsAp.execParser $ OptsAp.info optionsParser
                          $ OptsAp.progDesc "Cut a subset of columns from a DBF file"
  contents <- maybe ByteString.getContents ByteString.readFile path
  let dbf = case DBase.parse contents of
              Left err -> error err
              Right parsed -> parsed
      columns =  ASCII.split ',' (ASCII.pack cols)
  ByteString.putStr $ fromMaybe (error "can't re-serialize")
    $ DBase.serialize $ DBase.project ((keep ==) . (`elem` columns)) dbf
