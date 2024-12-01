{-# LANGUAGE FlexibleInstances, OverloadedStrings, ScopedTypeVariables, StandaloneDeriving, TemplateHaskell, TypeOperators #-}

module DBase (
  -- * Functions
  DBase.parse, DBase.serialize, project, csvHeader, csvRecords, headerFromCsv, recordFromCSV,
  -- * Types
  DBaseFile(..), FileHeader(..), Record(..), FieldType(..), FieldValue(..)) where

import Control.Arrow ((&&&))
import Control.Monad (join)
import Data.Functor.Identity (Identity(Identity, runIdentity))
import Data.Word (Word8, Word16, Word32, Word64)
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.ByteString.Char8 as ASCII
import qualified Data.Char as Char
import qualified Data.List as List
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import qualified Data.Csv as CSV
import Data.Scientific (Scientific, FPFormat(Fixed), formatScientific, isInteger)
import Data.Serialize
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Time.Calendar as Calendar
import qualified Rank2
import qualified Rank2.TH
import Text.Parser.Input (InputParsing, ParserInput)
import Text.ParserCombinators.Incremental.LeftBiasedLocal (Parser, inspect, feed, feedEof)
import Text.Read (readMaybe)
import Data.Vector (Vector)
import qualified Data.Vector as Vector
import Construct
import Construct.Classes (AlternativeFail)

import Prelude hiding ((*>), (<*), (<$), take, takeWhile)


data DBaseFile f = DBaseFile{
  header :: FileHeader f,
  records :: [Record f]}

data FileHeader f = FileHeader{
  signature :: f Word8,
  lastUpdate :: f ByteString,
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
  fieldDescriptors :: f [FieldDescriptor Identity]}
  --fieldProperties :: FieldProperties f}

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
               deriving (Bounded, Eq, Enum, Ord, Show)

data FieldValue = BinaryValue !ByteString
                | CharacterValue !ByteString
                | DateValue !ByteString
                | NumericValue !Scientific
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

deriving instance Show (DBaseFile Identity)
deriving instance Show (FileHeader Identity)
deriving instance Show (FieldDescriptor Identity)
deriving instance Show (FieldProperties Identity)
deriving instance Show (Record Identity)


parse :: Lazy.ByteString -> Either String (DBaseFile Identity)
parse input = fst <$> join (maybe (Left "success with no results") Right . listToMaybe . fst
                            <$> inspect (feedEof $ feed input $ Construct.parse file))

serialize :: DBaseFile Identity -> Maybe Lazy.ByteString
serialize = Construct.serialize file

-- | Project the subset of columns (i.e. filter the columns) from the dBASE file
project :: (ByteString -> Bool) -> DBaseFile Identity -> DBaseFile Identity
project pred DBaseFile{header = hdr@FileHeader{fieldDescriptors = Identity fields}, records = recs} =
  DBaseFile{header = fixHeaderLength hdr{fieldDescriptors = Identity $ projectList fields},
            records = map projectRecord recs}
  where projectRecord r@Record{fields = Identity values} = r{fields = Identity $ projectList values}
        projectList = mapMaybe (\(keep, x)-> if keep then Just x else Nothing) . zip keeps
        keeps = pred . runIdentity . fieldName <$> fields

csvHeader :: DBaseFile Identity -> CSV.Header
csvHeader = Vector.fromList . map (runIdentity . fieldName) . runIdentity . fieldDescriptors . header

csvRecords :: DBaseFile Identity -> [CSV.Record]
csvRecords = map (Vector.fromList . map csvField . runIdentity . fields) . filter (not . runIdentity . deleted) . records

headerFromCsv :: CSV.Header -> Calendar.Day -> Vector CSV.Record -> Either String (FileHeader Identity)
headerFromCsv hdr updated rs = fixHeaderLength <$> Rank2.traverse (Identity <$>) FileHeader{
  signature = Right 0x3,
  lastUpdate = encodeDate updated,
  recordCount = Right (fromIntegral $ Vector.length rs),
  headerLength = Right 0,
  recordLength = succ . sum . map (fromIntegral . runIdentity . fieldLength) <$> descriptors,
  reserved1 = Right 0,
  incompleteTransaction = Right False,
  encrypted = Right False,
  freeRecordThread = Right 0,
  reserved2 = Right 0,
  mdxTableFlag = Right False,
  codePage = Right 1,
  reserved3 = Right 0,
  fieldDescriptors = descriptors}
  where descriptors = descriptorsFromCsv hdr rs

fixHeaderLength :: FileHeader Identity -> FileHeader Identity
fixHeaderLength h = h{headerLength = Identity $ fromIntegral $ maybe 0 ByteString.length
                                              $ Construct.serialize fileHeader h}

recordFromCSV :: FileHeader Identity -> CSV.Record -> Either String (Record Identity)
recordFromCSV hdr r =
  fmap (Record (Identity False) . Identity) $
  sequenceA $
  zipWith (fieldFromCSV . runIdentity . fieldType) (runIdentity $ fieldDescriptors hdr) (Vector.toList r)

fieldFromCSV :: FieldType -> ByteString -> Either String FieldValue
fieldFromCSV BinaryType v = Right $ BinaryValue v
fieldFromCSV CharacterType v = Right $ CharacterValue v
fieldFromCSV NumberType v =
  maybe (Left ("Bad number " <> show v)) (Right . NumericValue) $ readMaybe $ ASCII.unpack $ ASCII.dropWhile (== ' ') v
fieldFromCSV LogicalType v
  | v == "?" = Right $ LogicalValue Nothing
  | v `elem` ["f", "F", "n", "N"] = Right $ LogicalValue (Just False)
  | v `elem` ["t", "T", "y", "Y"] = Right $ LogicalValue (Just True)
fieldFromCSV DateType v = Right $ DateValue $ ASCII.filter (/= '-') v

descriptorsFromCsv :: CSV.Header -> Vector CSV.Record -> Either String [FieldDescriptor Identity]
descriptorsFromCsv hdr rs
  | length columns /= length hdr = Left ("Number of columns (" <> show (length columns) <>
                                         ") doesn't match with the header (" <> shows (length hdr) ")")
  | otherwise = sequence $ zipWith fieldDescriptorFromCsv (Vector.toList hdr) columns
  where columns = List.transpose (Vector.toList <$> Vector.toList rs)

fieldDescriptorFromCsv :: ByteString -> [ByteString] -> Either String (FieldDescriptor Identity)
fieldDescriptorFromCsv name values = typedFieldDescriptorFromCsv name (inferType values) values

inferType :: [ByteString] -> FieldType
inferType values = head $ filter (`Set.member` foldr Set.intersection allTypes possibleTypeSets) typesByPreference
  where typesByPreference = [LogicalType, DateType, NumberType, CharacterType, MemoType,
                             FloatType, LongType, DoubleType, BinaryType]
        allTypes = Set.fromList [minBound .. maxBound]
        possibleTypeSets = possibleTypeSet <$> values

possibleTypeSet :: ByteString -> Set FieldType
possibleTypeSet value = Set.fromList $ concat [
  [CharacterType | ASCII.all Char.isAscii value],
  [LogicalType | value `elem` ["?", "f", "t", "F", "T", "y", "n", "Y", "N"]],
  [DateType | ByteString.length value == 8, ASCII.all Char.isDigit value,
              let monthDay = ByteString.drop 4 value, monthDay >= "0101", monthDay <= "1231",
              let day = ByteString.drop 2 monthDay, day <= "31"],
  [NumberType | let trimmed = ASCII.dropWhile (== ' ') value;
                    absolute = fromMaybe trimmed $ ASCII.stripPrefix "-" trimmed;
                    parts = ASCII.split '.' absolute,
                absolute /= "",
                ByteString.length trimmed < 18,
                length parts < 3, all (ASCII.all Char.isDigit) parts],
  [LongType | let trimmed = ASCII.dropWhile (== ' ') value;
                  absolute = fromMaybe trimmed $ ASCII.stripPrefix "-" trimmed,
              ASCII.all Char.isDigit absolute, absolute <= "2147483647"],
  [BinaryType, MemoType]]

typedFieldDescriptorFromCsv :: ByteString -> FieldType -> [ByteString] -> Either String (FieldDescriptor Identity)
typedFieldDescriptorFromCsv name ty values = Rank2.traverse (Identity <$>) FieldDescriptor{
  fieldName = if ByteString.length name < 11 then Right name else Left ("Field name too long: " <> show name),
  fieldType = Right ty,
  fieldLength = maxFieldLength ty values,
  fieldDecimals = maxFieldDecimals ty values,
  workAreaID = Right 1,
  setFieldsFlag = Right False,
  mdxFieldFlag = Right False}

maxFieldLength :: FieldType -> [ByteString] -> Either String Word8
maxFieldLength CharacterType values
  | width < 0xFFFF = Right (fromIntegral $ width `mod` 256)
  | otherwise = Left ("Character field width " <> show width)
  where width = maximum (ByteString.length <$> values)
maxFieldLength NumberType values
  | width < 18 = Right $ fromIntegral width
  | otherwise = Left ("Number field width " <> show width)
  where width = maximum (ByteString.length <$> values)
maxFieldLength LogicalType _ = Right 1
maxFieldLength DateType _ = Right 8
maxFieldLength MemoType _ = Right 10
maxFieldLength FloatType _ = Right 20
maxFieldLength DoubleType _ = Right 8
maxFieldLength DateTimeType _ = Right 8
maxFieldLength LongType _ = Right 4
maxFieldLength AutoincrementType _ = Right 4
maxFieldLength TimestampType _ = Right 8
maxFieldLength ty _ = Left ("Don't know the field length for " <> show ty)

maxFieldDecimals :: FieldType -> [ByteString] -> Either String Word8
maxFieldDecimals CharacterType values
  | width < 0xFFFF = Right (fromIntegral $ width `div` 256)
  | otherwise = Left ("Character field width " <> show width)
  where width = maximum (ByteString.length <$> values)
maxFieldDecimals NumberType values
  | Left err <- prec = Left err
  | prec < Right 16 = fromIntegral <$> prec
  | otherwise = Left ("Number field precision " <> foldMap show prec)
  where prec = maximum <$> traverse decimals values
        decimals n = case ASCII.split '.' $ ASCII.dropWhileEnd (== ' ') n of
                       [_whole, fraction] -> Right $ ByteString.length fraction
                       [_whole] -> Right 0
                       [] -> Right 0
                       _ -> Left ("More than one decimal point in number " <> show n)

encodeDate :: Calendar.Day -> Either String ByteString
encodeDate date
  | let (year, month, day) = Calendar.toGregorian date, year >= 1900 && year < 2156
  = Right $ ByteString.pack $ [fromIntegral $ year - 1900, fromIntegral month, fromIntegral day]
  | otherwise = Left "update year out of range"

csvField :: FieldValue -> ByteString
csvField (BinaryValue bs) = bs
csvField (CharacterValue bs) = bs
csvField (DateValue bs) = bs
csvField (TimestampValue bs) = bs
csvField (NumericValue n) = ASCII.pack (formatScientific Fixed (if isInteger n then Just 0 else Nothing) n)
csvField (LongValue n) = ASCII.pack (show n)
csvField (AutoincrementValue n) = ASCII.pack (show n)
csvField (FloatValue n) = ASCII.pack (show n)
csvField (DoubleValue n) = ASCII.pack (show n)
csvField (LogicalValue Nothing) = "?"
csvField (LogicalValue (Just False)) = "f"
csvField (LogicalValue (Just True)) = "t"

file :: Format (Parser Lazy.ByteString) Maybe Lazy.ByteString (DBaseFile Identity)
file = mapValue (uncurry DBaseFile) (header &&& records) $
  (deppair (mapSerialized Lazy.fromStrict Lazy.toStrict fileHeader) $
   \FileHeader{recordCount = Identity n, fieldDescriptors = Identity descriptors}->
     count (fromIntegral n) $ mapSerialized Lazy.fromStrict Lazy.toStrict $ tableRecord descriptors)
  <* (literal (Lazy.singleton 0x1A) <?> "EOF marker")
--  <* eof

fileHeader :: Format (Parser ByteString) Maybe ByteString (FileHeader Identity)
fileHeader = mfix $ \self-> record FileHeader{
  signature = satisfy (`elem` [0x2, 0x3, 0x4, 0x5]) byte,
  lastUpdate = take 3,
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
  fieldDescriptors = manyTill fieldDescriptor (literal (ByteString.singleton 0xD) <?> "field descriptors terminator")}
{-
  fieldProperties = FieldProperties{
    standardPropertyCount = word16le,
    standardPropertiesStart = word16le,
    customPropertyCount = word16le,
    customPropertiesStart = word16le,
    referentialIntegrityPropertyCount = word16le,
    referentialIntegrityPropertiesStart = word16le,
    dataStart = word16le,
    totalSize = word16le,
    properties = take (fromIntegral $ totalSize $ fieldProperties self)}-}

fieldDescriptor :: Format (Parser ByteString) Maybe ByteString (FieldDescriptor Identity)
fieldDescriptor = record FieldDescriptor{
  fieldName = padded1 (ByteString.replicate 11 0) (takeWhile (\c-> c >= "\x01" && c < "\x80")) <?> "field name",
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
              <|> TimestampType <$ literal "@"
              <?> "field type",
  fieldLength = literal "\0\0\0\0" *> byte <?> "field length",
  fieldDecimals = satisfy (<= 15) byte <* literal "\0\0" <?> "field decimals",
  workAreaID = byte <* literal "\0\0",
  setFieldsFlag = flag <* literal (ByteString.replicate 7 0),
  mdxFieldFlag = flag}
  

tableRecord :: [FieldDescriptor Identity] -> Format (Parser ByteString) Maybe ByteString (Record Identity)
tableRecord expected = record Record{
  deleted = True <$ value byte (fromIntegral $ Char.ord '*') <|> False <$ value byte (fromIntegral $ Char.ord ' '),
  fields = forF expected fieldValue}

fieldValue :: FieldDescriptor Identity -> Format (Parser ByteString) Maybe ByteString FieldValue
fieldValue FieldDescriptor{fieldType = Identity t, fieldLength = Identity len, fieldDecimals = Identity dec} = case t of
  CharacterType | let strLen = 256 * fromIntegral dec + fromIntegral len
                  -> mapMaybeValue
     (Just . CharacterValue . ASCII.dropWhileEnd (== ' '))
     (\(CharacterValue s)-> if ByteString.length s > strLen then Nothing
                            else Just $ s <> padding strLen s)
     (take strLen)
  NumberType -> mapMaybeValue
    (fmap NumericValue . readMaybe . ASCII.unpack . ASCII.dropWhile (== ' '))
    (Just . (\s-> padding (fromIntegral len) s <> s) . trimInsignificantZeros dec . ASCII.pack
     . formatScientific Fixed (Just $ fromIntegral dec) . \(NumericValue x)-> x)
    (take $ fromIntegral len)
  FloatType -> mapMaybeValue
    (fmap FloatValue . readMaybe . ASCII.unpack . ASCII.dropWhile (== ' '))
    (Just . ASCII.pack . show . \(FloatValue x)-> x)
    (take $ fromIntegral len)
  where trimInsignificantZeros 0 = id
        trimInsignificantZeros _ = ASCII.dropWhileEnd (== '.') . ASCII.dropWhileEnd (== '0')

padding :: Int -> ByteString -> ByteString
padding targetLength s = ASCII.replicate (targetLength - ByteString.length s) ' '

word16le :: Format (Parser ByteString) Maybe ByteString Word16
word16le = cereal' getWord16le putWord16le

flag :: Format (Parser ByteString) Maybe ByteString Bool
flag = False <$ value byte 0 <|> True <$ value byte 1

forF :: forall m n s a b. (Monad m, AlternativeFail n, InputParsing m, ParserInput m ~ s, Monoid s, Eq b, Show b)
     => [a] -> (a -> Format m n s b) -> Format m n s [b]
forF (x : xs) f = mapMaybeValue (Just . uncurry (:)) List.uncons $ pair (f x) (forF xs f)
forF [] _ = [] <$ literal (mempty :: s)
