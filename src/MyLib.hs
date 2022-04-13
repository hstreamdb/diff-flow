{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE StandaloneDeriving #-}

module MyLib where

import Control.Monad
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

deriving instance (Eq a) => Eq (Timestamp a)
deriving instance (Ord a) => Ord (Timestamp a)

class CausalOrd a b where
  causalCompare :: a -> b -> PartialOrdering

instance (Ord a) => CausalOrd (Timestamp a) (Timestamp a) where
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

infix 4 <.=
(<.=) :: (CausalOrd a b) => a -> b -> Bool
(<.=) x y
  | compRes == PLT = True
  | compRes == PEQ = True
  | otherwise      = False
  where compRes = x `causalCompare` y

----
type Frontier a = MultiSet (Timestamp a)

instance (Ord a) => CausalOrd (Frontier a) (Timestamp a) where
  causalCompare ft ts =
    MultiSet.fold (\x acc -> if acc == PNONE then x `causalCompare` ts else acc) PNONE ft

data MoveDirection = MoveLater | MoveEarlier deriving (Show, Eq, Enum, Read)
data FrontierChange a = FrontierChange
  { frontierChangeTs :: Timestamp a
  , frontierChangeDiff :: Int
  }

-- Move later: remove timestamps that are earlier than ts.
-- Move earlier: remove timestamps that are later than ts.
-- FIXME: when to stop or error?
moveFrontier :: (Ord a, Show a)
             => Frontier a -> MoveDirection -> Timestamp a
             -> (Frontier a, [FrontierChange a])
moveFrontier ft direction ts = (MultiSet.insert ts ft', FrontierChange ts 1 : changes)
  where
    changes = case direction of
      MoveLater   ->
         MultiSet.fold (\x acc -> case x `causalCompare` ts of
           PEQ   -> error $ "Already moved to " <> show ts <> "? Found " <> show x
           PGT   -> error $ "Already moved to " <> show ts <> "? Found " <> show x
           PLT   -> let change = FrontierChange x (-1) in change:acc
           PNONE -> acc) [] ft
      MoveEarlier ->
        MultiSet.fold (\x acc -> case x `causalCompare` ts of
           PEQ   -> error $ "Already moved to " <> show ts <> "? Found " <> show x
           PLT   -> error $ "Already moved to " <> show ts <> "? Found " <> show x
           PGT   -> let change = FrontierChange x (-1) in change:acc
           PNONE -> acc) [] ft
    ft' = L.foldr (\FrontierChange{..} acc -> MultiSet.delete frontierChangeTs acc) ft changes

----

data TimestampsWithFrontier a = TimestampsWithFrontier
  { tsfTimestamps :: MultiSet (Timestamp a)
  , tsfFrontier :: Frontier a
  }

updateTimestampsWithFrontier :: (Ord a)
                             => TimestampsWithFrontier a
                             -> Timestamp a
                             -> Int
                             -> (TimestampsWithFrontier a, [FrontierChange a])
updateTimestampsWithFrontier TimestampsWithFrontier{..} ts diff =
  case MultiSet.occur ts timestampsInserted of
    -- an item in tsfTimestamps has been removed
    0 -> case MultiSet.member ts tsfFrontier of
           -- the frontier is unmodified.
           False -> let tsf'   = TimestampsWithFrontier timestampsInserted tsfFrontier
                     in (tsf', [])
           -- the item is also removed from the frontier, new items may be required
           -- to be inserted to the frontier to keep [frontier <.= each ts]
           True  -> let change = FrontierChange ts (-1)
                        frontierRemoved = MultiSet.delete ts tsfFrontier
                        frontierAdds = MultiSet.filter (\x -> frontierRemoved `causalCompare` x == PNONE) (MultiSet.filter (\x -> x `causalCompare` ts == PGT) tsfTimestamps)
                        frontierChanges = L.map (\x -> FrontierChange x 1) (MultiSet.toList frontierAdds)
                        frontierInserted = MultiSet.fold MultiSet.insert frontierRemoved frontierAdds
                        tsf' = TimestampsWithFrontier timestampsInserted frontierInserted
                     in (tsf', change:frontierChanges)
    -- the item was not present but now got inserted. it is new!
    diff -> case tsfFrontier `causalCompare` ts of
              -- the invariant [frontier <.= each ts] still keeps
              PLT -> let tsf' = TimestampsWithFrontier timestampsInserted tsfFrontier
                      in (tsf', [])
              -- the invariant [frontier <.= each ts] is broken, which means
              -- the new-added item should be added to the frontier to keep
              -- it. However, every item in the frontier is incomparable so
              -- then some redundant items should be deleted from the frontier
              otherwise -> let change = FrontierChange ts 1
                               frontierInserted = MultiSet.insert ts tsfFrontier
                               frontierRemoves = MultiSet.filter (\x -> x `causalCompare` ts == PGT) frontierInserted
                               frontierChanges = L.map (\x -> FrontierChange x (-1)) (MultiSet.toList frontierRemoves)
                               frontierRemoved = MultiSet.fold MultiSet.delete frontierInserted frontierRemoves
                               tsf' = TimestampsWithFrontier timestampsInserted frontierRemoved
                            in (tsf', change:frontierChanges)
  where timestampsInserted = MultiSet.insertMany ts diff tsfTimestamps
