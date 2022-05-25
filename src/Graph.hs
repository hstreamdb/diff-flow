{-# LANGUAGE DeriveAnyClass  #-}
{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module Graph where

import           Types

import           Control.Concurrent.MVar
import           Data.Aeson              (Value (..))
import           Data.Set                (Set)
import qualified Data.Set                as Set
import           Data.Word               (Word64)


import           Data.Hashable           (Hashable)
import           Data.HashMap.Lazy       (HashMap)
import qualified Data.HashMap.Lazy       as HM
import qualified Data.List               as L
import           Data.Proxy
import           Data.Vector             (Vector)
import qualified Data.Vector             as V
import           GHC.Generics            (Generic)
import           Control.Concurrent.STM

newtype Node = Node { nodeId :: Int } deriving (Eq, Show, Ord, Generic, Hashable)

data NodeInput = NodeInput
  { nodeInputNode  :: Node
  , nodeInputIndex :: Int
  } deriving (Eq, Show, Ord, Generic, Hashable)

newtype Mapper = Mapper { mapper :: Row -> Row }
newtype Filter = Filter { filterF :: Row -> Bool }
newtype Joiner = Joiner { joiner :: Row -> Row -> Row }
newtype Reducer = Reducer { reducer :: Row -> Row -> Row } -- \acc x -> acc'
type KeyGenerator = Row -> Row

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
  | FilterSpec        Node Filter               -- input, filter
  | IndexSpec         Node                      -- input
  | JoinSpec          Node Node KeyGenerator KeyGenerator Joiner -- input1, input2, keygen1, keygen2, joiner
  | OutputSpec        Node                      -- input
  | TimestampPushSpec Node                      -- input
  | TimestampIncSpec  (Maybe Node)              -- input
  | TimestampPopSpec  Node                      -- input
  | UnionSpec         Node Node                 -- input1, input2
  | DistinctSpec      Node                      -- input
  | ReduceSpec        Node Row KeyGenerator Reducer -- input, init, kengen, reducer

instance Show NodeSpec where
  show InputSpec = "InputSpec"
  show (MapSpec _ _) = "MapSpec"
  show (FilterSpec _ _) = "FilterSpec"
  show (IndexSpec _) = "IndexSpec"
  show (JoinSpec _ _ _ _ _) = "JoinSpec"
  show (OutputSpec _) = "OutputSpec"
  show (TimestampPushSpec _) = "TimestampPushSpec"
  show (TimestampIncSpec _) = "TimestampIncSpec"
  show (TimestampPopSpec _) = "TimestampPopSpec"
  show (UnionSpec _ _) = "UnionSpec"
  show (DistinctSpec _) = "DistinctSpec"
  show (ReduceSpec _ _ _ _) = "ReduceSpec"

outputIndex :: NodeSpec -> Bool
outputIndex (IndexSpec _)        = True
outputIndex (DistinctSpec _)     = True
outputIndex (ReduceSpec _ _ _ _) = True
outputIndex _                    = False

needIndex :: NodeSpec -> Bool
needIndex (DistinctSpec _)     = True
needIndex (ReduceSpec _ _ _ _) = True
needIndex _                    = False

getInputsFromSpec :: NodeSpec -> Vector Node
getInputsFromSpec InputSpec = V.empty
getInputsFromSpec (MapSpec node _) = V.singleton node
getInputsFromSpec (FilterSpec node _) = V.singleton node
getInputsFromSpec (IndexSpec node) = V.singleton node
getInputsFromSpec (JoinSpec node1 node2 _ _ _) = V.fromList [node1, node2]
getInputsFromSpec (OutputSpec node) = V.singleton node
getInputsFromSpec (TimestampPushSpec node) = V.singleton node
getInputsFromSpec (TimestampIncSpec m_node) = case m_node of
                                               Nothing   -> V.empty
                                               Just node -> V.singleton node
getInputsFromSpec (TimestampPopSpec node) = V.singleton node
getInputsFromSpec (UnionSpec node1 node2) = V.fromList [node1, node2]
getInputsFromSpec (DistinctSpec node) = V.singleton node
getInputsFromSpec (ReduceSpec node _ _ _) = V.singleton node


data NodeState a
  = InputState (TVar (Frontier a)) (TVar (DataChangeBatch a))
  | IndexState (TVar (Index a)) (TVar [DataChange a])
  | JoinState (TVar (Frontier a)) (TVar (Frontier a))
  | OutputState (TVar [DataChangeBatch a])
  | DistinctState (TVar (Index a)) (TVar (HashMap Row (Set (Timestamp a))))
  | ReduceState (TVar (Index a)) (TVar (HashMap Row (Set (Timestamp a))))
  | NoState

instance Show (NodeState a) where
  show (InputState _ _)    = "InputState"
  show (IndexState _ _)    = "IndexState"
  show (JoinState _ _)     = "JoinState"
  show (OutputState _)     = "OutputState"
  show (DistinctState _ _) = "DistinctState"
  show (ReduceState _ _)   = "ReduceState"
  show NoState             = "NodeState"

getIndexFromState :: NodeState a -> TVar (Index a)
getIndexFromState (IndexState    index_m _) = index_m
getIndexFromState (DistinctState index_m _) = index_m
getIndexFromState (ReduceState   index_m _) = index_m
getIndexFromState _ = error "Trying getting index from a node which does not contains index"

specToState :: (Show a, Ord a, Hashable a) => NodeSpec -> IO (NodeState a)
specToState InputSpec = do
  frontier <- newTVarIO Set.empty
  unflushedChanges <- newTVarIO $ mkDataChangeBatch []
  return $ InputState frontier unflushedChanges
specToState (IndexSpec _) = do
  index <- newTVarIO $ Index []
  pendingChanges <- newTVarIO []
  return $ IndexState index pendingChanges
specToState (JoinSpec _ _ _ _ _) = do
  frontier1 <- newTVarIO Set.empty
  frontier2 <- newTVarIO Set.empty
  return $ JoinState frontier1 frontier2
specToState (OutputSpec _) = do
  unpopedBatches <- newTVarIO []
  return $ OutputState unpopedBatches
specToState (DistinctSpec _) = do
  index <- newTVarIO $ Index []
  pendingCorrections <- newTVarIO HM.empty
  return $ DistinctState index pendingCorrections
specToState (ReduceSpec _ _ _ _) = do
  index <- newTVarIO $ Index []
  pendingCorrections <- newTVarIO HM.empty
  return $ ReduceState index pendingCorrections
specToState _ = return NoState


----

newtype Subgraph = Subgraph { subgraphId :: Int } deriving (Eq, Show, Generic, Hashable)

data Graph = Graph
  { graphNodeSpecs       :: HashMap Int NodeSpec
  , graphNodeSubgraphs   :: HashMap Int [Subgraph]
  , graphSubgraphParents :: HashMap Int Subgraph
  , graphDownstreamNodes :: HashMap Int [NodeInput]
  }

data GraphBuilder = GraphBuilder
  { graphBuilderNodeSpecs       :: Vector NodeSpec
  , graphBuilderSubgraphs       :: Vector Subgraph
  , graphBuilderSubgraphParents :: Vector Subgraph
  }

emptyGraphBuilder :: GraphBuilder
emptyGraphBuilder = GraphBuilder V.empty V.empty V.empty

addSubgraph :: GraphBuilder -> Subgraph -> (GraphBuilder, Subgraph)
addSubgraph builder@GraphBuilder{..} parent =
  ( builder{ graphBuilderSubgraphParents = V.snoc graphBuilderSubgraphParents parent}
  , Subgraph {subgraphId = V.length graphBuilderSubgraphParents + 1}
  )

addSubgraph' :: GraphBuilder -> Subgraph -> GraphBuilder
addSubgraph' builder parent = fst $ addSubgraph builder parent

addNode :: GraphBuilder -> Subgraph -> NodeSpec -> (GraphBuilder, Node)
addNode builder@GraphBuilder{..} subgraph spec =
  ( builder{ graphBuilderNodeSpecs = V.snoc graphBuilderNodeSpecs spec
           , graphBuilderSubgraphs = V.snoc graphBuilderSubgraphs subgraph}
  , newNode
  )
  where newNode = Node { nodeId = V.length graphBuilderNodeSpecs }

addNode' :: GraphBuilder -> Subgraph -> NodeSpec -> GraphBuilder
addNode' builder sub spec = fst $ addNode builder sub spec

connectLoop :: GraphBuilder -> Node -> Node -> GraphBuilder
connectLoop builder@GraphBuilder{..} later earlier =
  builder{ graphBuilderNodeSpecs =
           case graphBuilderNodeSpecs V.! earlierId of
             TimestampIncSpec _ ->
               V.update graphBuilderNodeSpecs
                 (V.singleton (earlierId, TimestampIncSpec (Just later)))
             _ -> error "connectLoop: the earlier node can only be TimestampInc"
         }
  where earlierId = nodeId earlier

buildGraph :: GraphBuilder -> Graph
buildGraph GraphBuilder{..} =
  if V.length graphBuilderSubgraphs == nodesNum then
    Graph { graphNodeSpecs = nodeSpecs
          , graphNodeSubgraphs = subgraphs
          , graphSubgraphParents = subgraphParents
          , graphDownstreamNodes = downstreamNodes
          }
    else error $ "GraphBuilder: NodeSpecs and Subgraphs have different length: "
               <> show nodesNum <> ", " <> show (V.length graphBuilderSubgraphs)
  where
    nodesNum = V.length graphBuilderNodeSpecs
    nodeSpecs = V.ifoldl (\acc i x -> HM.insert i x acc) HM.empty graphBuilderNodeSpecs
    findSubgraphs :: Subgraph -> [Subgraph]
    findSubgraphs immSubgraph
      | immId == 0 = [immSubgraph]
      | otherwise = immSubgraph:findSubgraphs (graphBuilderSubgraphParents V.! (immId - 1))
      where immId = subgraphId immSubgraph
    subgraphs = V.ifoldl (\acc i x ->
                                 HM.insert i (findSubgraphs x) acc)
                     HM.empty graphBuilderSubgraphs
    downstreamNodes = V.ifoldl (\acc i x ->
                                 V.ifoldl (\acc' i' x' ->
                                             let nodeInput = NodeInput (Node i) i'
                                              in HM.adjust (nodeInput :) (nodeId x') acc'
                                          ) acc (getInputsFromSpec x)
                               )
                      (V.ifoldl (\acc i x -> HM.insert i [] acc) HM.empty  graphBuilderNodeSpecs)
                      graphBuilderNodeSpecs
    subgraphParents = V.ifoldl (\acc i x -> HM.insert i x acc) HM.empty graphBuilderSubgraphParents
