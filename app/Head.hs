{-# LANGUAGE Haskell2010, OverloadedRecordDot, NamedFieldPuns #-}
module Main where

import qualified Construct
import Control.Applicative ((<|>), (<**>), optional)
import Data.Functor.Identity (Identity(Identity, runIdentity))
import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Lazy as ByteString
import DBase (DBaseFile(..), FileHeader(recordCount))
import qualified DBase
import Options.Applicative (long, metavar, short)
import qualified Options.Applicative as OptsAp
import Text.ParserCombinators.Incremental.LeftBiasedLocal (inspect, feed, feedEof)


optionsParser :: OptsAp.Parser (Int, Maybe FilePath)
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
  let (header, padding, recordBytes) = case DBase.parseHeader contents of
        Left err -> error ("parseHeader: " <> err)
        Right parsed -> parsed
      recordsPrefix = case inspect $ feedEof $ feed recordsPrefixBytes $ Construct.parse recordsPrefixFormat of
        Right ([(parsed, rest)], Nothing) | ByteString.null rest -> parsed
        Left err -> error ("recordsPrefix: " <> err)
        _ -> error "Unexpected parse result"
      recordsPrefixBytes = ByteString.take (fromIntegral keep * fromIntegral header.recordLength) recordBytes
      recordsPrefixFormat = Construct.count keep singleRecordFormat
      singleRecordFormat = Construct.serializedByteStringFromStrict (DBase.record header.fieldDescriptors.runIdentity)
  ByteString.putStr $ fromMaybe (error "can't re-serialize")
    $ DBase.serialize DBaseFile{header = header{recordCount = min (Identity $ fromIntegral keep) $ recordCount header},
                                postHeaderPadding = padding,
                                records = recordsPrefix}
