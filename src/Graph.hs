{-# LANGUAGE GADTs #-}

module Graph where

import MyLib

import Data.Word (Word64)
import Data.Aeson (Value (..))

import Data.Proxy

newtype Node = Node { nodeId :: Word64 } deriving (Eq, Show, Ord)

data NodeInput = NodeInput
  { nodeInputNode :: Node
  , nodeInputIndex :: Word64
  } deriving (Eq, Show, Ord)

newtype Mapper = Mapper { mapper :: Row -> Row }
newtype Reducer = Reducer { reducer :: Value -> Row -> Word64 -> Row }

{-
class HasIndex a where
  hasIndex :: Proxy a

class NeedIndex a where
  needIndex :: Proxy a

data NodeSpec a where
  InputSpec :: NodeSpec a
  MapSpec :: Node -> Mapper -> NodeSpec a
  IndexSpec :: (HasIndex a) => Node -> NodeSpec a
  ReduceSpec :: (HasIndex a, NeedIndex a) => Node -> Word64 -> Value -> Reducer -> NodeSpec a
-}

data NodeSpec
  = InputSpec
  | MapSpec           Node Mapper               -- input, mapper
  | IndexSpec         Node                      -- input
  | JoinSpec          Node Node Word64          -- input1, input2, key_columns
  | OutputSpec        Node                      -- input
  | TimestampPushSpec Node                      -- input
  | TimestampIncSpec  (Maybe Node)              -- input
  | TimestampPopSpec  Node                      -- input
  | UnionSpec         Node Node                 -- input1, input2
  | DistinctSpec      Node                      -- input
  | ReduceSpec        Node Word64 Value Reducer -- input, key_columns, init, reducer

outputIndex :: NodeSpec -> Bool
outputIndex (IndexSpec _)        = True
outputIndex (DistinctSpec _)     = True
outputIndex (ReduceSpec _ _ _ _) = True
outputIndex _ = False

needIndex :: NodeSpec -> Bool
needIndex (DistinctSpec _)     = True
needIndex (ReduceSpec _ _ _ _) = True
needIndex _ = False

getInpusFromSpec :: NodeSpec -> [Node]
getInpusFromSpec = undefined
