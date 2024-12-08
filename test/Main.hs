module Main (main) where

import Data.Maybe (isJust)
import DBase
import DBaseGen
import Hedgehog.Main (defaultMain)
import Hedgehog

main :: IO ()
main = defaultMain (check <$> properties)

properties :: [Property]
properties = [
  property $ forAll dbf >>= assert . isJust . serialize,
  property $ do x <- forAll dbf
                let Just s = serialize x
                parse s === Right x]
