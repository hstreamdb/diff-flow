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
      (builder_3, node_2) = addNode builder_2 subgraph_0 (OutputSpec node_1)
  let graph = buildGraph builder_3

  shard <- buildShard graph
  forkIO $ run shard

  pushInput shard node_1 (DataChange [String "aaa"] (Timestamp (0 :: Word32) []) 1)
  pushInput shard node_1 (DataChange [String "bbb"] (Timestamp (0 :: Word32) []) 1)
  flushInput shard node_1
  advanceInput shard node_1 (Timestamp 1 [])

  threadDelay 10000000
