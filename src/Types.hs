{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Types where

import           Control.Monad
import           Data.Aeson        (Object (..), Value (..))
import qualified Data.Aeson        as Aeson
import qualified Data.HashMap.Lazy as HM
import qualified Data.List         as L
import           Data.MultiSet     (MultiSet)
import qualified Data.MultiSet as MultiSet
import           Data.Vector       (Vector)
import qualified Data.Vector       as V
import           Data.Word         (Word64)

import Data.Set (Set)
import qualified Data.Set as Set
import Data.Hashable (Hashable)
import GHC.Generics (Generic)

type Row = [Value]
type Bag = MultiSet Row

data PartialOrdering = PLT | PEQ | PGT | PNONE deriving (Eq, Show)

data Timestamp a = Timestamp
  { timestampTime   :: a
  , timestampCoords :: [Word64] -- [outermost <--> innermost]
  }

instance (Show a) => Show (Timestamp a) where
  show Timestamp{..} = "<" <> show timestampTime
                           <> "|"
                           <> L.intercalate "," (L.map show timestampCoords) <> ">"

deriving instance (Eq a) => Eq (Timestamp a)
deriving instance (Ord a) => Ord (Timestamp a)
deriving instance Generic (Timestamp a)
deriving instance (Hashable a) => Hashable (Timestamp a)

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
type Frontier a = Set (Timestamp a)

instance (Ord a) => CausalOrd (Frontier a) (Timestamp a) where
  causalCompare ft ts =
    Set.foldl (\acc x -> if acc == PNONE then x `causalCompare` ts else acc) PNONE ft

data MoveDirection = MoveLater | MoveEarlier deriving (Show, Eq, Enum, Read)
data FrontierChange a = FrontierChange
  { frontierChangeTs   :: Timestamp a
  , frontierChangeDiff :: Int
  }

deriving instance (Eq a) => Eq (FrontierChange a)
deriving instance (Ord a) => Ord (FrontierChange a)
instance (Show a) => Show (FrontierChange a) where
  show FrontierChange{..} = "(" <> show frontierChangeTs
                                <> ", " <> show frontierChangeDiff
                                <> ")"



-- Move later: remove timestamps that are earlier than ts.
-- Move earlier: remove timestamps that are later than ts.
-- FIXME: when to stop or error?
moveFrontier :: (Ord a, Show a)
             => Frontier a -> MoveDirection -> Timestamp a
             -> (Frontier a, [FrontierChange a])
moveFrontier ft direction ts = (Set.insert ts ft', FrontierChange ts 1 : changes)
  where
    (_, changes) = case direction of
      MoveLater   ->
        Set.foldl (\(goOn,acc) x ->
          case goOn of
            False -> (goOn,acc)
            True  -> case x `causalCompare` ts of
              PEQ   -> if L.null acc then (False,acc)
                         else error $
                              "Already moved to " <> show ts <> "? Found " <> show x
              PGT   -> if L.null acc then (False,acc)
                         else error $
                              "Already moved to " <> show ts <> "? Found " <> show x
              PLT   -> let change = FrontierChange x (-1) in (goOn, change:acc)
              PNONE -> (goOn,acc)) (True,[]) ft
      MoveEarlier ->
        Set.foldl (\(goOn,acc) x ->
          case goOn of
            False -> (goOn,acc)
            True  -> case x `causalCompare` ts of
              PEQ   -> if L.null acc then (False,acc)
                         else error $
                              "Already moved to " <> show ts <> "? Found " <> show x
              PLT   -> if L.null acc then (False,acc)
                         else error $
                              "Already moved to " <> show ts <> "? Found " <> show x
              PGT   -> let change = FrontierChange x (-1) in (goOn, change:acc)
              PNONE -> (goOn,acc)) (True,[]) ft
    ft' = L.foldl (\acc FrontierChange{..} -> Set.delete frontierChangeTs acc) ft changes

infixl 7 ~>>
(~>>) :: (Ord a, Show a) => Frontier a -> (MoveDirection, Timestamp a) -> Frontier a
(~>>) ft (direction,ts) = fst $ moveFrontier ft direction ts
----

data TimestampsWithFrontier a = TimestampsWithFrontier
  { tsfTimestamps :: MultiSet (Timestamp a)
  , tsfFrontier   :: Frontier a
  }

instance (Show a) => Show (TimestampsWithFrontier a) where
  show TimestampsWithFrontier{..} = "[\n\tTimestamps: " <> show tsfTimestamps
                                  <> "\n\tFrontier: " <> show tsfFrontier
                                  <> "\n]"

updateTimestampsWithFrontier :: (Ord a)
                             => TimestampsWithFrontier a
                             -> Timestamp a
                             -> Int
                             -> (TimestampsWithFrontier a, [FrontierChange a])
updateTimestampsWithFrontier TimestampsWithFrontier{..} ts diff
  -- an item in tsfTimestamps has been removed
  | MultiSet.occur ts timestampsInserted == 0 =
    case Set.member ts tsfFrontier of
      -- the frontier is unmodified.
      False -> let tsf'   = TimestampsWithFrontier timestampsInserted tsfFrontier
               in (tsf', [])
      -- the item is also removed from the frontier, new items may be required
      -- to be inserted to the frontier to keep [frontier <.= each ts]
      True  -> let change = FrontierChange ts (-1)
                   frontierRemoved = Set.delete ts tsfFrontier
                   frontierAdds = MultiSet.toSet $ MultiSet.filter (\x -> frontierRemoved `causalCompare` x == PNONE) (MultiSet.filter (\x -> x `causalCompare` ts == PGT) tsfTimestamps)
                   frontierChanges = L.map (\x -> FrontierChange x 1) (Set.toList frontierAdds)
                   frontierInserted = Set.foldl (flip Set.insert) frontierRemoved frontierAdds
                   tsf' = TimestampsWithFrontier timestampsInserted frontierInserted
                in (tsf', change:frontierChanges)
    -- the item was not present but now got inserted. it is new!
  | MultiSet.occur ts timestampsInserted == diff =
    case tsfFrontier `causalCompare` ts of
      -- the invariant [frontier <.= each ts] still keeps
      PLT -> let tsf' = TimestampsWithFrontier timestampsInserted tsfFrontier
              in (tsf', [])
      -- the invariant [frontier <.= each ts] is broken, which means
      -- the new-added item should be added to the frontier to keep
      -- it. However, every item in the frontier is incomparable so
      -- then some redundant items should be deleted from the frontier
      _   -> let change = FrontierChange ts 1
                 frontierInserted = Set.insert ts tsfFrontier
                 frontierRemoves = Set.filter (\x -> x `causalCompare` ts == PGT) frontierInserted
                 frontierChanges = L.map (\x -> FrontierChange x (-1)) (Set.toList frontierRemoves)
                 frontierRemoved = Set.foldl (flip Set.delete) frontierInserted frontierRemoves
                 tsf' = TimestampsWithFrontier timestampsInserted frontierRemoved
              in (tsf', change:frontierChanges)
  | otherwise = let tsf' = TimestampsWithFrontier timestampsInserted tsfFrontier
                 in (tsf', [])
  where timestampsInserted = MultiSet.insertMany ts diff tsfTimestamps

infixl 7 ->>
(->>) :: (Ord a)
      => TimestampsWithFrontier a
      -> (Timestamp a, Int)
      -> TimestampsWithFrontier a
(->>) tsf (ts,diff) = fst $ updateTimestampsWithFrontier tsf ts diff

----

data DataChange a = DataChange
  { dcRow :: Row
  , dcTimestamp :: Timestamp a
  , dcDiff :: Int
  }
deriving instance (Eq a) => Eq (DataChange a)
deriving instance (Ord a) => Ord (DataChange a)
deriving instance (Show a) => Show (DataChange a)

data DataChangeBatch a = DataChangeBatch
  { dcbLowerBound :: Frontier a
  , dcbChanges :: [DataChange a] -- sorted and de-duplicated
  }
deriving instance (Eq a) => Eq (DataChangeBatch a)
deriving instance (Ord a) => Ord (DataChangeBatch a)
deriving instance (Show a) => Show (DataChangeBatch a)

emptyDataChangeBatch :: DataChangeBatch a
emptyDataChangeBatch = DataChangeBatch {dcbLowerBound=Set.empty, dcbChanges=[]}

dataChangeBatchLen :: DataChangeBatch a -> Int
dataChangeBatchLen DataChangeBatch{..} = L.length dcbChanges

mkDataChangeBatch :: (Hashable a, Ord a, Show a)
                  => [DataChange a]
                  -> DataChangeBatch a
mkDataChangeBatch changes = DataChangeBatch frontier sortedChanges
  where getKey DataChange{..} = (dcRow, dcTimestamp)
        coalescedChanges = HM.filter (\DataChange{..} -> dcDiff /= 0) $
          L.foldl (\acc x -> HM.insertWith
                    (\new old -> old {dcDiff = dcDiff new + dcDiff old} )
                    (getKey x) x acc) HM.empty changes
        sortedChanges = L.sort $ HM.elems coalescedChanges
        frontier = L.foldl
          (\acc DataChange{..} -> acc ~>> (MoveEarlier,dcTimestamp))
          Set.empty sortedChanges

----

newtype Index a = Index
  { indexChangeBatches :: [DataChangeBatch a]
  }
deriving instance (Eq a) => Eq (Index a)
deriving instance (Ord a) => Ord (Index a)
deriving instance (Show a) => Show (Index a)

addChangeBatchToIndex :: (Hashable a, Ord a, Show a)
                      => Index a
                      -> DataChangeBatch a
                      -> Index a
addChangeBatchToIndex Index{..} changeBatch =
  Index (adjustBatches $ indexChangeBatches ++ [changeBatch])
  where
    adjustBatches [] = []
    adjustBatches [x] = [x]
    adjustBatches l@(x:y:xs)
      | dataChangeBatchLen lastBatch * 2 <= dataChangeBatchLen secondLastBatch = l
      | otherwise =
        let newBatch = mkDataChangeBatch (dcbChanges lastBatch ++ dcbChanges secondLastBatch)
         in adjustBatches ((L.init . L.init $ l) ++ [newBatch])
      where lastBatch = L.last l
            secondLastBatch = L.last . L.init $ l
