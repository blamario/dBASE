{-# LANGUAGE FlexibleInstances, OverloadedStrings, ScopedTypeVariables, StandaloneDeriving, TemplateHaskell, TypeOperators #-}

module DBase (
  -- * Functions
  DBase.parse, DBase.serialize, csvHeader, csvRecords,
  -- * Types
  DBaseFile(..), FileHeader(..), Record(..), FieldType(..), FieldValue(..)) where

import Control.Arrow ((&&&))
import Data.Functor.Identity (Identity(Identity, runIdentity))
import Data.Word (Word8, Word16, Word32, Word64)
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.ByteString.Char8 as ASCII
import qualified Data.Char as Char
import qualified Data.List as List
import Data.Maybe (listToMaybe)
import qualified Data.Csv as CSV
import Data.Scientific (FPFormat(Fixed), formatScientific)
import Data.Serialize
import qualified Rank2.TH
import Text.Parser.Input (InputParsing, ParserInput)
import Text.ParserCombinators.Incremental.LeftBiasedLocal (Parser, completeResults, feed, feedEof)
import Text.Read (readMaybe)
import qualified Data.Vector as Vector
import Construct
import Construct.Classes (AlternativeFail)

import Prelude hiding ((*>), (<*), (<$), take, takeWhile)


data DBaseFile f = DBaseFile{
  header :: FileHeader f,
  records :: [Record f]}

data FileHeader f = FileHeader{
  signature :: f Word8,
  recordCount :: f Word32,
  headerLength :: f Word16,
  recordLength :: f Word16,
  reserved1 :: f Word16,
  incompleteTransaction :: f Bool,
  encrypted :: f Bool,
  freeRecordThread :: f Word32,
  reserved2 :: f Word64,
  mdxTableFlag :: f Bool,
  codePage :: f Word8,
  reserved3 :: f Word16,
  fieldDescriptors :: f [FieldDescriptor Identity],
  fieldProperties :: FieldProperties f}

data FieldDescriptor f = FieldDescriptor{
  fieldName :: f ByteString,
  fieldType :: f FieldType,
  -- 4 bytes of offset or memory address,
  fieldLength :: f Word8,
  fieldDecimals :: f Word8,
  -- reserved1f :: f Word16,
  workAreaID :: f Word8,
  -- reserved2f :: f Word16,
  setFieldsFlag :: f Bool,
  mdxFieldFlag :: f Bool}

data FieldProperties f = FieldProperties{
  standardPropertyCount :: f Word16,
  standardPropertiesStart :: f Word16,
  customPropertyCount :: f Word16,
  customPropertiesStart :: f Word16,
  referentialIntegrityPropertyCount :: f Word16,
  referentialIntegrityPropertiesStart :: f Word16,
  dataStart :: f Word16,
  totalSize :: f Word16,
  properties :: f ByteString}

data Record f = Record{
  deleted :: f Bool,
  fields :: f [FieldValue]}

data FieldType = CharacterType
               | NumberType
               | LogicalType
               | DateType
               | MemoType
               | FloatType
               | DoubleType
               | BinaryType
               | OLEType
               | PictureType
               | CurrencyType
               | DateTimeType
               | LongType
               | AutoincrementType
               | VarCharType
               | TimestampType
               deriving (Eq, Show)

data FieldValue = BinaryValue !ByteString
                | CharacterValue !ByteString
                | DateValue !ByteString
                | NumericValue !Rational
                | LogicalValue !(Maybe Bool)
                | Memo !ByteString
                | TimestampValue !ByteString
                | LongValue !Int
                | AutoincrementValue !Int
                | FloatValue !Float
                | DoubleValue !Double
                | OLE !ByteString
                deriving (Eq, Show)

$(Rank2.TH.deriveAll ''Record)
$(Rank2.TH.deriveAll ''FieldProperties)
$(Rank2.TH.deriveAll ''FieldDescriptor)
$(Rank2.TH.deriveAll ''FileHeader)

deriving instance Show (Record Identity)


parse :: Lazy.ByteString -> Maybe (DBaseFile Identity)
parse input = fmap fst $ listToMaybe $ completeResults $ feedEof $ feed input $ Construct.parse file

serialize :: DBaseFile Identity -> Maybe Lazy.ByteString
serialize = Construct.serialize file

csvHeader :: DBaseFile Identity -> CSV.Header
csvHeader = Vector.fromList . map (runIdentity . fieldName) . runIdentity . fieldDescriptors . header

csvRecords :: DBaseFile Identity -> [CSV.Record]
csvRecords = map (Vector.fromList . map fieldBS . runIdentity . fields) . filter (not . runIdentity . deleted) . records

fieldBS :: FieldValue -> ByteString
fieldBS (BinaryValue bs) = bs
fieldBS (CharacterValue bs) = bs
fieldBS (DateValue bs) = bs
fieldBS (TimestampValue bs) = bs
fieldBS (NumericValue n) = ASCII.pack (formatScientific Fixed Nothing $ fromRational n)
fieldBS (LongValue n) = ASCII.pack (show n)
fieldBS (AutoincrementValue n) = ASCII.pack (show n)
fieldBS (FloatValue n) = ASCII.pack (show n)
fieldBS (DoubleValue n) = ASCII.pack (show n)
fieldBS (LogicalValue Nothing) = mempty
fieldBS (LogicalValue (Just False)) = "f"
fieldBS (LogicalValue (Just True)) = "t"

file :: Format (Parser Lazy.ByteString) Maybe Lazy.ByteString (DBaseFile Identity)
file = mapValue (uncurry DBaseFile) (header &&& records) $
  (deppair (mapSerialized Lazy.fromStrict Lazy.toStrict fileHeader) $
   \FileHeader{recordCount = Identity n, fieldDescriptors = Identity descriptors}->
     count (fromIntegral n) $ mapSerialized Lazy.fromStrict Lazy.toStrict $ tableRecord descriptors)
  <* literal (Lazy.singleton 0x1A)
--  <* eof

fileHeader :: Format (Parser ByteString) Maybe ByteString (FileHeader Identity)
fileHeader = mfix $ \self-> record FileHeader{
  signature = satisfy (`elem` [0x2, 0x3, 0x4, 0x5]) byte,
  recordCount = cereal' getWord32le putWord32le,
  headerLength = word16le,
  recordLength = word16le,
  reserved1 = cereal,
  incompleteTransaction = flag,
  encrypted = flag,
  freeRecordThread = cereal,
  reserved2 = cereal,
  mdxTableFlag = flag,
  codePage = byte,
  reserved3 = cereal,
  fieldDescriptors = many fieldDescriptor <* literal (ByteString.singleton 0xD),
  fieldProperties = FieldProperties{
    standardPropertyCount = word16le,
    standardPropertiesStart = word16le,
    customPropertyCount = word16le,
    customPropertiesStart = word16le,
    referentialIntegrityPropertyCount = word16le,
    referentialIntegrityPropertiesStart = word16le,
    dataStart = word16le,
    totalSize = word16le,
    properties = take (fromIntegral $ totalSize $ fieldProperties self)}}

fieldDescriptor :: Format (Parser ByteString) Maybe ByteString (FieldDescriptor Identity)
fieldDescriptor = record FieldDescriptor{
  fieldName = padded1 (ByteString.replicate 11 0) $ takeWhile (\c-> c >= "\x01" && c < "\x80"),
  fieldType = CharacterType <$ literal "C"
              <|> NumberType <$ literal "N"
              <|> LogicalType <$ literal "L"
              <|> DateType <$ literal "D"
              <|> MemoType <$ literal "M"
              <|> FloatType <$ literal "F"
              <|> DoubleType <$ literal "O"
              <|> BinaryType <$ literal "B"
              <|> OLEType <$ literal "G"
              <|> PictureType <$ literal "P"
              <|> CurrencyType <$ literal "Y"
              <|> DateTimeType <$ literal "T"
              <|> LongType <$ literal "I"
              <|> AutoincrementType <$ literal "+"
              <|> VarCharType <$ literal "V"
              <|> TimestampType <$ literal "@",
  fieldLength = literal "\0\0\0\0" *> byte,
  fieldDecimals = satisfy (<= 15) byte <* literal "\0\0",
  workAreaID = byte <* literal "\0\0",
  setFieldsFlag = flag <* literal (ByteString.replicate 7 0),
  mdxFieldFlag = flag}
  

tableRecord :: [FieldDescriptor Identity] -> Format (Parser ByteString) Maybe ByteString (Record Identity)
tableRecord expected = record Record{
  deleted = True <$ value byte (fromIntegral $ Char.ord '*') <|> True <$ value byte (fromIntegral $ Char.ord ' '),
  fields = forF expected fieldValue}

fieldValue :: FieldDescriptor Identity -> Format (Parser ByteString) Maybe ByteString FieldValue
fieldValue FieldDescriptor{fieldType = Identity t, fieldLength = Identity len, fieldDecimals = Identity dec} = case t of
  CharacterType | let strLen = 256 * fromIntegral dec + fromIntegral len
                  -> mapMaybeValue
     (Just . CharacterValue . ASCII.dropWhileEnd (== ' '))
     (\(CharacterValue s)-> if ByteString.length s > strLen then Nothing
                            else Just $ s <> ASCII.replicate (strLen - ASCII.length s) ' ')
     (take strLen)
  NumberType -> mapMaybeValue
    (fmap NumericValue . readMaybe . ASCII.unpack)
    (Just . ASCII.pack . formatScientific Fixed (Just $ fromIntegral dec) . fromRational . \(NumericValue x)-> x)
    (take $ fromIntegral len)

word16le :: Format (Parser ByteString) Maybe ByteString Word16
word16le = cereal' getWord16le putWord16le

flag :: Format (Parser ByteString) Maybe ByteString Bool
flag = False <$ value byte 0 <|> True <$ value byte 1

forF :: forall m n s a b. (Monad m, AlternativeFail n, InputParsing m, ParserInput m ~ s, Monoid s, Eq b, Show b)
     => [a] -> (a -> Format m n s b) -> Format m n s [b]
forF (x : xs) f = mapMaybeValue (Just . uncurry (:)) List.uncons $ pair (f x) (forF xs f)
forF [] _ = [] <$ literal (mempty :: s)
