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
  let mapper = Mapper (\row -> let (String s) = head row in [String (s <> "_mapped")])
  let (builder_3, node_2) = addNode builder_2 subgraph_0 (MapSpec node_1 mapper)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (OutputSpec node_2)
  let graph = buildGraph builder_4

  shard <- buildShard graph
  forkIO $ run shard
  forkIO . forever $ popOutput shard node_3

  pushInput shard node_1 (DataChange [String "aaa"] (Timestamp (0 :: Word32) []) 1)
  pushInput shard node_1 (DataChange [String "bbb"] (Timestamp (0 :: Word32) []) 1)
  flushInput shard node_1
  advanceInput shard node_1 (Timestamp 1 [])

  threadDelay 10000000
