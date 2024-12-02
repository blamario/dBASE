{-# LANGUAGE Haskell2010, TupleSections #-}
module Main where

import Control.Applicative ((<**>))
import Data.Foldable1 (foldlM1)
import Data.Functor.Identity (Identity(Identity, runIdentity))
import Data.List.NonEmpty (NonEmpty, some1)
import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Lazy as ByteString
import Data.Time.Clock (getCurrentTime, utctDay)
import DBase (parse, serialize, zipWithM, encodeDate, FileHeader(..), Record(..))
import Options.Applicative (Parser, metavar)
import qualified Options.Applicative as OptsAp


optionsParser :: Parser (NonEmpty String)
optionsParser = some1 (OptsAp.argument OptsAp.str (metavar "DBF file" <> OptsAp.action "file"))
                <**> OptsAp.helper

main :: IO ()
main = do
  paths <- OptsAp.execParser $ OptsAp.info optionsParser $ OptsAp.progDesc "Zip together DBF files with equal numbers of records"
  contents <- traverse ByteString.readFile paths
  Right today <- encodeDate . utctDay <$> getCurrentTime
  let dbfs = case traverse DBase.parse contents of
               Left err -> error err
               Right parsed -> parsed
      joinHeaders h1@FileHeader{signature = s1, recordCount = c1, fieldDescriptors = fs1}
                     FileHeader{signature = s2, recordCount = c2, fieldDescriptors = fs2}
        | s1 /= s2 = error ("Different DBF version signatures " <> show (s1, s2))
        | c1 /= c2 = error ("Unequal DBF record counts: " <> show (c1, c2))
        | otherwise = pure h1{fieldDescriptors = fs1 <> fs2, lastUpdate = Identity today}
      joinRecords Record{deleted = d1, fields = fs1} Record{deleted = d2, fields = fs2}
        | d1 /= d2 = error "Disagreement on whether a record is deleted"
        | otherwise = pure Record{deleted = d1, fields = fs1 <> fs2}
  ByteString.putStr $ fromMaybe (error "can't re-serialize")
                    $ DBase.serialize $ runIdentity $ foldlM1 (DBase.zipWithM joinHeaders joinRecords) dbfs
