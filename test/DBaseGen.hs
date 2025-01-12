{-# LANGUAGE Haskell2010 #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DBaseGen where

import Control.Applicative (empty)
import Control.Monad (replicateM)
import Data.Functor.Identity
import Data.Scientific (scientific)
import Data.Word
import Data.Time.Calendar qualified as Calendar

import DBase

import Data.ByteString.Char8 qualified as ByteString
import Data.Time.Calendar (fromGregorian)
import Rank2 qualified

import Hedgehog qualified
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range

dbf :: Hedgehog.Gen (DBaseFile Identity)
dbf = do h@FileHeader{recordCount = Identity n, fieldDescriptors = Identity fields} <- DBaseGen.header
         DBaseFile h <$> Gen.bytes (Range.linear 0 100) <*> replicateM (fromIntegral n) (record fields)

header :: Hedgehog.Gen (FileHeader Identity)
header = fixHeaderLength . fixRecordLength <$> Rank2.traverse (Identity <$>) FileHeader{
  signature = pure 0x3,
  lastUpdate = Gen.enum (fromGregorian 1900 1 1) (fromGregorian 2155 12 31),
  recordCount = Gen.word32 (Range.linear 0 1000),
  headerLength = pure 0,
  recordLength = pure 0,
  reserved1 = Gen.enumBounded,
  incompleteTransaction = Gen.enumBounded,
  encrypted = pure False,
  freeRecordThread = Gen.enumBounded,
  reserved2 = Gen.word64 Range.linearBounded,
  mdxTableFlag = Gen.enumBounded,
  codePage = Gen.enumBounded,
  reserved3 = Gen.enumBounded,
  fieldDescriptors = Gen.list (Range.exponential 1 255) fieldDescriptor}

fieldDescriptor :: Hedgehog.Gen (FieldDescriptor Identity)
fieldDescriptor = do t <- DBaseGen.fieldType
                     lengthOfFieldType t >>= fieldDescriptorOfType t

fieldDescriptorOfType :: FieldType -> Word8 -> Hedgehog.Gen (FieldDescriptor Identity)
fieldDescriptorOfType t len = Rank2.traverse (Identity <$>) FieldDescriptor{
  fieldName = Gen.utf8 (Range.linear 1 10) Gen.alphaNum,
  DBase.fieldType = pure t,
  fieldLength = pure len,
  fieldDecimals = case t of
      CharacterType -> Gen.word8 (Range.exponential 0 2)
      NumberType | len == 0 -> empty
                 | len == 1 -> pure 0
                 | otherwise -> Gen.word8 (Range.linear 0 (min 15 $ len-2))
      _ -> pure 0,
  reserved1f = Gen.word16 Range.linearBounded,
  workAreaID = Gen.enumBounded,
  reserved2f = Gen.word16 Range.linearBounded,
  setFieldsFlag = Gen.enumBounded,
  reserved3f = Gen.bytes (Range.singleton 7),
  mdxFieldFlag = Gen.enumBounded}

lengthOfFieldType :: FieldType -> Hedgehog.Gen Word8
lengthOfFieldType CharacterType = Gen.word8 (Range.exponential 0 255)
lengthOfFieldType NumberType = Gen.word8 (Range.linear 1 18)
lengthOfFieldType LogicalType = pure 1
lengthOfFieldType DateType = pure 8

fieldType :: Hedgehog.Gen FieldType
fieldType = Gen.element [CharacterType, NumberType, LogicalType, DateType]

record :: [FieldDescriptor Identity] -> Hedgehog.Gen (Record Identity)
record fds = Rank2.traverse (Identity <$>) Record{
  deleted = Gen.enumBounded,
  fields = traverse fieldValue fds}

fieldValue :: FieldDescriptor Identity -> Hedgehog.Gen FieldValue
fieldValue FieldDescriptor{DBase.fieldType = Identity t, fieldLength = Identity len, fieldDecimals = Identity d} =
  case t of
    CharacterType ->
      CharacterValue . ByteString.dropWhileEnd (== ' ') <$> Gen.utf8 (Range.linear 0 (256*d' + len')) Gen.ascii
    NumberType ->
      NumericValue <$>
      if d == 0 then flip scientific 0 <$> Gen.integral (Range.linearFrom (1 - 10^(len-1)) 0 (10^len-1))
      else flip scientific (- fromIntegral d) <$> Gen.integral (Range.linearFrom (1 - 10^(len-d-2)) 0 (10^(len-d-1) - 1))
    LogicalType -> LogicalValue <$> Gen.maybe Gen.enumBounded
    DateType -> DateValue <$> Gen.enum (fromGregorian 1 1 1) (fromGregorian 9999 12 31)
  where d' = fromIntegral d
        len' = fromIntegral len
