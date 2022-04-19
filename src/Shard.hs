{-# LANGUAGE RecordWildCards #-}

module Shard where

import           Graph
import           Types

import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Data.Hashable           (Hashable)
import qualified Data.HashMap.Lazy       as HM
import qualified Data.Set                as Set
import qualified Data.Vector             as V

data ChangeBatchAtNodeInput a = ChangeBatchAtNodeInput
  { cbiChangeBatch   :: DataChangeBatch a
  , cbiInputFrontier :: Maybe (Frontier a)
  , cbiNodeInput     :: NodeInput
  }

data Pointstamp a = Pointstamp
  { pointstampNodeInput :: NodeInput
  , pointstampSubgraphs :: [Subgraph]
  , pointstampTimestamp :: Timestamp a
  }

data Shard a = Shard
  { shardGraph                      :: Graph
  , shardNodeStates                 :: MVar (HM.HashMap Int (NodeState a))
  , shardNodeFrontiers              :: MVar (HM.HashMap Int (TimestampsWithFrontier a))
  , shardUnprocessedChangeBatches   :: MVar [ChangeBatchAtNodeInput a]
  , shardUnprocessedFrontierUpdates :: MVar (HM.HashMap (Pointstamp a) Int)
  }

----

--
--  DataChange -> |INPUT NODE|
--                 (unflushed)
pushInput :: (Hashable a, Ord a, Show a) => Shard a -> Node -> DataChange a -> IO ()
pushInput Shard{..} Node{..} change = do
  shardNodeStates' <- readMVar shardNodeStates
  case HM.lookup nodeId shardNodeStates' of
    Nothing -> error $ "No matching node found: " <> show nodeId
    Just (InputState frontier_m unflushedChanges_m) -> do
      frontier <- readMVar frontier_m
      case frontier <.= (dcTimestamp change) of
        False -> error $ "Can not push inputs whose ts < frontier of Input Node."
        True  -> modifyMVar_ unflushedChanges_m
                   (\batch -> return $ updateDataChangeBatch batch (\xs -> xs ++ [change]))
    Just state -> error $ "Incorrect type of node state found: " <> show state

--
--  |INPUT NODE| -> ...
--  (unflushed)
flushInput :: (Ord a) => Shard a -> Node -> IO ()
flushInput shard@Shard{..} node@Node{..} = do
  shardNodeStates' <- readMVar shardNodeStates
  case HM.lookup nodeId shardNodeStates' of
    Nothing -> error $ "No matching node found: " <> show nodeId
    Just (InputState frontier_m unflushedChanges_m) -> do
      unflushedChangeBatch <- readMVar unflushedChanges_m
      undefined
      --emitChangeBatch shard node unflushedChangeBatch
    Just state -> error $ "Incorrect type of node state found: " <> show state

--
--           Timestamp         -> |INPUT NODE| -> ...
--  (update frontier to this)
advanceInput :: (Ord a, Show a) => Shard a -> Node -> Timestamp a -> IO ()
advanceInput shard@Shard{..} node@Node{..} ts = do
  flushInput shard node
  shardNodeStates' <- readMVar shardNodeStates
  case HM.lookup nodeId shardNodeStates' of
    Nothing -> error $ "No matching node found: " <> show nodeId
    Just (InputState frontier_m unflushedChanges_m) -> do
      frontier <- readMVar frontier_m
      let (newFrontier, ftChanges) = moveFrontier frontier MoveLater ts
      putMVar frontier_m newFrontier
      -- mapM_ (\change -> applyFrontierChange shard node (frontierChangeTs change) (frontierChangeDiff change)) ftChanges
    Just state -> error $ "Incorrect type of node state found: " <> show state

emitChangeBatch :: (Ord a) => Shard a -> Node -> DataChangeBatch a -> IO ()
emitChangeBatch shard@Shard{..} node dcb@DataChangeBatch{..} = do
  case HM.lookup (nodeId node) (graphNodeSpecs shardGraph) of
    Nothing   -> error $ "No matching node found: " <> show (nodeId node)
    Just spec -> do
      case outputIndex spec of
        True -> unless (V.length (getInpusFromSpec spec) == 1) $ do
          error "Nodes that output indexes can only have 1 input"
        False -> return ()

      -- check emission: frontier of from_node <= ts
      shardNodeFrontiers' <- readMVar shardNodeFrontiers
      case HM.lookup (nodeId node) shardNodeFrontiers' of
        Nothing       -> error $ "No matching node found: " <> show (nodeId node)
        Just outputFt -> mapM_
          (\ts -> assert (tsfFrontier outputFt <.= ts) (return ())) dcbLowerBound

      -- emit to downstream
      let inputFt = if outputIndex spec then
            let inputNodeId = (nodeId . V.head) (getInpusFromSpec spec)
             in Just . tsfFrontier $ shardNodeFrontiers' HM.! inputNodeId
            else Nothing
          toNodeInputs = graphDownstreamNodes shardGraph HM.! nodeId node
      mapM_
        (\toNodeInput -> do
            -- mapM_ (\ts -> queueFrontierChange shard (nodeInputNode toNodeInput) 1) dcbLowerBound
            modifyMVar_ shardUnprocessedChangeBatches
              (\xs -> let newCbi = ChangeBatchAtNodeInput
                            { cbiChangeBatch = dcb
                            , cbiInputFrontier = inputFt
                            , cbiNodeInput = toNodeInput
                            }
                       in return $ xs ++ [newCbi]
              )
        ) toNodeInputs
