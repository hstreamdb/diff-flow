{-# LANGUAGE RecordWildCards #-}
module MyLib where

import qualified Data.List as L
import Data.Word (Word64)
import Data.Aeson (Object (..), Value (..))
import qualified Data.Aeson as Aeson
import qualified Data.HashMap.Lazy as HM
import qualified Data.Vector as V
import Data.Vector (Vector)
import Data.MultiSet (MultiSet)
import Data.MultiSet as MultiSet

type Row = Vector Value
type Bag = MultiSet Row

data PartialOrdering = PLT | PEQ | PGT | PNONE deriving (Eq, Show)

data Timestamp a = Timestamp
  { timestampTime :: a
  , timestampCoords :: [Word64] -- [outermost <--> innermost]
  }

instance (Show a) => Show (Timestamp a) where
  show Timestamp{..} = "<" <> show timestampTime
                           <> L.intercalate "," (L.map show timestampCoords) <> ">"

class CausalOrd a where
  causalCompare :: a -> a -> PartialOrdering

instance (Ord a) => CausalOrd (Timestamp a) where
  causalCompare ts1 ts2 =
    if len1 == len2 then
      if L.all (== EQ) compRes then PEQ
        else if L.all (\x -> x == EQ || x == LT) compRes then PLT
          else if L.all (\x -> x == EQ || x == GT) compRes then PGT
            else PNONE
      else error $ "Trying comparing timestamps with different lengths: " <> show len1 <> ", " <> show len2
    where len1 = L.length (timestampCoords ts1)
          len2 = L.length (timestampCoords ts2)
          compRes = (timestampTime ts1 `compare` timestampTime ts2) :
                    L.zipWith compare (timestampCoords ts1) (timestampCoords ts2)

leastUpperBound :: (Ord a) => Timestamp a -> Timestamp a -> Timestamp a
leastUpperBound ts1 ts2 =
  Timestamp { timestampTime = upperTime, timestampCoords = upperCoords }
  where upperTime = max (timestampTime ts1) (timestampTime ts2)
        upperCoords = L.zipWith max (timestampCoords ts1) (timestampCoords ts2)

----
type Frontier a = MultiSet (Timestamp a)
