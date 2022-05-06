{-# LANGUAGE OverloadedStrings  #-}

module Main where

import Types
import Graph
import Shard

import Control.Concurrent
import Control.Monad
import Data.Aeson (Value (..))
import Data.Word
import qualified Data.HashMap.Lazy as HM

main :: IO ()
main = do
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, node_2) = addNode builder_2 subgraph_0 InputSpec
  let (builder_4, node_1') = addNode builder_3 subgraph_0 (IndexSpec node_1)
      (builder_5, node_2') = addNode builder_4 subgraph_0 (IndexSpec node_2)


  let keygen = \[k1,v1,k2,v2] -> [k2,v2]
      rowgen = \[k1,v1,k2,v2] [k1',v1',k2',v2'] -> [k1,v1,k2,v2, k1',v1',k2',v2']
  let (builder_6, node_3) = addNode builder_5 subgraph_0 (JoinSpec node_1' node_2' keygen (Joiner rowgen))

  let (builder_7, node_4) = addNode builder_6 subgraph_0 (OutputSpec node_3)

  let graph = buildGraph builder_7

  shard <- buildShard graph
  forkIO $ run shard
  forkIO . forever $ popOutput shard node_4

  pushInput shard node_1
    (DataChange [String "a", Number 1, String "b", Number 2] (Timestamp (0 :: Word32) []) 1)
  pushInput shard node_2
    (DataChange [String "c", Number 3, String "b", Number 2] (Timestamp (0 :: Word32) []) 1)

  flushInput shard node_1
  flushInput shard node_2
  advanceInput shard node_1 (Timestamp 1 [])
  advanceInput shard node_2 (Timestamp 1 [])

  threadDelay 10000000
