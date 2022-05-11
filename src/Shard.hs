{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}

module Shard where

import           Graph
import           Types

import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Data.Hashable           (Hashable)
import qualified Data.HashMap.Lazy       as HM
import qualified Data.List               as L
import Data.Set (Set)
import qualified Data.Set                as Set
import qualified Data.MultiSet           as MultiSet
import qualified Data.Vector             as V
import GHC.Generics (Generic)
import Control.Concurrent (threadDelay)
import Data.Foldable.Extra (findM)
import Data.Maybe (isNothing, fromJust)
import           Control.Concurrent.STM
import qualified Data.Tuple as Tuple

data ChangeBatchAtNodeInput a = ChangeBatchAtNodeInput
  { cbiChangeBatch   :: DataChangeBatch a
  , cbiInputFrontier :: Maybe (Frontier a)
  , cbiNodeInput     :: NodeInput
  }
deriving instance (Show a) => Show (ChangeBatchAtNodeInput a)

data Pointstamp a = Pointstamp
  { pointstampNodeInput :: NodeInput
  , pointstampSubgraphs :: [Subgraph]
  , pointstampTimestamp :: Timestamp a
  }
deriving instance (Eq a) => Eq (Pointstamp a)
deriving instance Generic (Pointstamp a)
deriving instance (Hashable a) => Hashable (Pointstamp a)

instance (Ord a) => Ord (Pointstamp a) where
  compare ps1 ps2 =
    case subgraph1 of
      []     -> pointstampNodeInput ps1 `compare` pointstampNodeInput ps2
      (x:xs) ->
        case subgraph2 of
          []     -> pointstampNodeInput ps1 `compare` pointstampNodeInput ps2
          (y:ys) ->
            if x /= y then
              pointstampNodeInput ps1 `compare` pointstampNodeInput ps2 else
              let tsOrd = timestampTime (pointstampTimestamp ps1) `compare`
                          timestampTime (pointstampTimestamp ps2)
               in case tsOrd of
                    EQ -> let newTs1 = Timestamp
                                       (head . timestampCoords $ pointstampTimestamp ps1)
                                       (tail . timestampCoords $ pointstampTimestamp ps1)
                              newTs2 = Timestamp
                                       (head . timestampCoords $ pointstampTimestamp ps2)
                                       (tail . timestampCoords $ pointstampTimestamp ps2)
                              newPs1 = ps1 { pointstampSubgraphs = xs
                                           , pointstampTimestamp = newTs1
                                           }
                              newPs2 = ps2 { pointstampSubgraphs = ys
                                           , pointstampTimestamp = newTs2
                                           }
                           in newPs1 `compare` newPs2
                    _  -> tsOrd
    where subgraph1 = pointstampSubgraphs ps1
          subgraph2 = pointstampSubgraphs ps2

data Shard a = Shard
  { shardGraph                      :: Graph
  , shardNodeStates                 :: MVar (HM.HashMap Int (NodeState a))
  , shardNodeFrontiers              :: MVar (HM.HashMap Int (TimestampsWithFrontier a))
  , shardUnprocessedChangeBatches   :: MVar [ChangeBatchAtNodeInput a]
  , shardUnprocessedFrontierUpdates :: MVar (HM.HashMap (Pointstamp a) Int)
  }


buildShard :: (Hashable a, Ord a, Show a, Bounded a) => Graph -> IO (Shard a)
buildShard graph@Graph{..} = do
  hmStateList <- mapM (\(k,v) -> do
                          state <- specToState v
                          return (k,state)
                      )(HM.toList graphNodeSpecs)
  states <- newMVar $ HM.fromList hmStateList
  frontiers <- newMVar $ HM.map (\_ -> emptyTimestampsWithFrontier) graphNodeSpecs
  unprocessedChangeBatches <- newMVar []
  unprocessedFrontierUpdates <- newMVar HM.empty

  let shard = Shard { shardGraph = graph
                    , shardNodeStates = states
                    , shardNodeFrontiers = frontiers
                    , shardUnprocessedChangeBatches = unprocessedChangeBatches
                    , shardUnprocessedFrontierUpdates = unprocessedFrontierUpdates
                    }
  mapM_ (\(i,spec) -> case spec of
                    InputSpec -> do
                      let inputSubgraphs = graphNodeSubgraphs HM.! i
                      shardNodeStates' <- readMVar (shardNodeStates shard)
                      let (InputState ft_m _) = shardNodeStates' HM.! i
                      let ts = leastTimestamp (L.length inputSubgraphs - 1)
                      applyFrontierChange shard (Node i) ts 1
                      atomically $ modifyTVar ft_m (Set.insert ts)
                    _         -> return ()
        ) (HM.toList graphNodeSpecs)
  return shard

----

--
--  DataChange -> |INPUT NODE|
--                 (unflushed)
pushInput :: (Hashable a, Ord a, Show a) => Shard a -> Node -> DataChange a -> IO ()
pushInput Shard{..} Node{..} change = do
  shardNodeStates' <- readMVar shardNodeStates
  case HM.lookup nodeId shardNodeStates' of
    Nothing -> error $ "No matching node found: " <> show nodeId
    Just (InputState frontier_m unflushedChanges_m) -> atomically $ do
      frontier <- readTVar frontier_m
      case frontier <.= (dcTimestamp change) of
        False -> error $ "Can not push inputs whose ts < frontier of Input Node. Frontier = " <> show frontier <> ", ts = " <> show (dcTimestamp change)
        True  -> modifyTVar unflushedChanges_m
                   (\batch -> updateDataChangeBatch batch (\xs -> xs ++ [change]))
    Just state -> error $ "Incorrect type of node state found: " <> show state

--
--  |INPUT NODE| -> ...
--  (unflushed)
flushInput :: (Hashable a, Ord a, Show a) => Shard a -> Node -> IO ()
flushInput shard@Shard{..} node@Node{..} = do
  shardNodeStates' <- readMVar shardNodeStates
  case HM.lookup nodeId shardNodeStates' of
    Nothing -> error $ "No matching node found: " <> show nodeId
    Just (InputState frontier_m unflushedChanges_m) -> do
      unflushedChangeBatch <- atomically $ swapTVar unflushedChanges_m emptyDataChangeBatch
      unless (L.null $ dcbChanges unflushedChangeBatch) $
        emitChangeBatch shard node unflushedChangeBatch
    Just state -> error $ "Incorrect type of node state found: " <> show state

--
--           Timestamp         -> |INPUT NODE| -> ...
--  (update frontier to this)
advanceInput :: (Hashable a, Ord a, Show a) => Shard a -> Node -> Timestamp a -> IO ()
advanceInput shard@Shard{..} node@Node{..} ts = do
  flushInput shard node
  shardNodeStates' <- readMVar shardNodeStates
  case HM.lookup nodeId shardNodeStates' of
    Nothing -> error $ "No matching node found: " <> show nodeId
    Just (InputState frontier_m _) -> do
      ftChanges <- atomically $ do
        stateTVar frontier_m (\ft -> Tuple.swap $ moveFrontier ft MoveLater ts)
      mapM_ (\change -> applyFrontierChange shard node (frontierChangeTs change) (frontierChangeDiff change)) ftChanges
    Just state -> error $ "Incorrect type of node state found: " <> show state

emitChangeBatch :: (Hashable a, Ord a, Show a) => Shard a -> Node -> DataChangeBatch a -> IO ()
emitChangeBatch shard@Shard{..} node dcb@DataChangeBatch{..} = do
  print $ "Emitting from node " <> show node <> " with DataChangeBatch: " <> show dcb
  case HM.lookup (nodeId node) (graphNodeSpecs shardGraph) of
    Nothing   -> error $ "No matching node found: " <> show (nodeId node)
    Just spec -> do
      case outputIndex spec of
        True -> unless (V.length (getInputsFromSpec spec) == 1) $ do
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
            let inputNodeId = (nodeId . V.head) (getInputsFromSpec spec)
             in Just . tsfFrontier $ shardNodeFrontiers' HM.! inputNodeId
            else Nothing
          toNodeInputs = graphDownstreamNodes shardGraph HM.! nodeId node
      mapM_
        (\toNodeInput -> do
            -- ???: Is it correct? I do not see any reason to update the
            -- input frontier(which then updates the output frontier!)
            -- Also line 177
            -- Remove both of them seems still OK...
            mapM_ (\ts -> queueFrontierChange shard toNodeInput ts 1) dcbLowerBound
            let newCbi = ChangeBatchAtNodeInput
                         { cbiChangeBatch = dcb
                         , cbiInputFrontier = inputFt
                         , cbiNodeInput = toNodeInput
                         }
            modifyMVar_ shardUnprocessedChangeBatches (\xs -> return $ xs ++ [newCbi])
        ) toNodeInputs


processChangeBatch :: (Hashable a, Ord a, Show a) => Shard a -> IO ()
processChangeBatch shard@Shard{..} = do
  shardUnprocessedChangeBatches' <- readMVar shardUnprocessedChangeBatches
  case shardUnprocessedChangeBatches' of
    []      -> return ()
    (cbi:_) -> do
      modifyMVar_ shardUnprocessedChangeBatches (return . L.tail)
      let nodeInput   = cbiNodeInput cbi
          changeBatch = cbiChangeBatch cbi
          node = nodeInputNode nodeInput
      -- ???: Is it correct? I do not see any reason to update the
      -- input frontier(which then updates the output frontier!)
      -- Also line 150
      -- Remove both of them seems still OK...
      mapM_ (\ts -> queueFrontierChange shard nodeInput ts (-1)) (dcbLowerBound changeBatch)
      shardNodeStates'    <- readMVar shardNodeStates
      case graphNodeSpecs shardGraph HM.! nodeId node of
        InputSpec -> error $ "Input node will never have work to do on its input"
        MapSpec _ (Mapper mapper) -> do
          let outputChangeBatch = L.foldl
                (\acc change -> do
                    let outputRow = mapper (dcRow change)
                        newChange = DataChange
                          { dcRow = outputRow
                          , dcTimestamp = dcTimestamp change
                          , dcDiff = dcDiff change
                          }
                    updateDataChangeBatch acc (\xs -> xs ++ [newChange])
                ) emptyDataChangeBatch (dcbChanges changeBatch)
          unless (L.null $ dcbChanges outputChangeBatch) $
            emitChangeBatch shard node outputChangeBatch
        FilterSpec _ (Filter filter') -> do
          let outputChangeBatch = L.foldl
                (\acc change ->
                   if filter' (dcRow change) then
                     updateDataChangeBatch acc (\xs -> xs ++ [change]) else acc
                ) emptyDataChangeBatch (dcbChanges changeBatch)
          unless (L.null $ dcbChanges outputChangeBatch) $
            emitChangeBatch shard node outputChangeBatch
        IndexSpec _ -> do
          mapM_ (\change -> do
                    shardNodeFrontiers' <- readMVar shardNodeFrontiers
                    let nodeFrontier = tsfFrontier $ shardNodeFrontiers' HM.! nodeId node
                    assert (nodeFrontier <.= dcTimestamp change) (return ())
                    applyFrontierChange shard node (dcTimestamp change) 1
                ) (dcbChanges changeBatch)
          let (IndexState _ pendingChanges_m) = shardNodeStates' HM.! nodeId node
          atomically $
            modifyTVar pendingChanges_m (\xs -> xs ++ dcbChanges changeBatch)
        JoinSpec node1 node2 keygen1 keygen2 (Joiner joiner) -> do
          let inputIx = nodeInputIndex nodeInput
              otherNode = case inputIx of
                            0 -> node2
                            1 -> node1
                            _ -> error "impossible!"
          otherIndex <- readTVarIO $ getIndexFromState (shardNodeStates' HM.! nodeId otherNode)
          let (JoinState ft1_m ft2_m) = shardNodeStates' HM.! nodeId node
          joinFt <- case inputIx of
                      0 -> readTVarIO ft2_m
                      1 -> readTVarIO ft1_m
                      _ -> error "impossible!"
          let outputChangeBatch =
                mergeJoinIndex otherIndex joinFt changeBatch keygen1 keygen2 joiner
          unless (L.null $ dcbChanges outputChangeBatch) $
            emitChangeBatch shard node outputChangeBatch
          let inputFt = fromJust $ cbiInputFrontier cbi -- FIXME: unsafe
          case inputIx of
            0 -> atomically $ writeTVar ft1_m inputFt
            1 -> atomically $ writeTVar ft2_m inputFt
            _ -> error "impossible!"
        OutputSpec _ -> do
          let (OutputState unpoppedChangeBatches_m) = shardNodeStates' HM.! nodeId node
          atomically $ modifyTVar unpoppedChangeBatches_m (\xs -> xs ++ [changeBatch])
        TimestampPushSpec _ -> do
          let outputChangeBatch = L.foldl
                (\acc change -> do
                    let outputTs  = pushCoord (dcTimestamp change)
                        newChange = DataChange
                          { dcRow = dcRow change
                          , dcTimestamp = outputTs
                          , dcDiff = dcDiff change
                          }
                    updateDataChangeBatch acc (\xs -> xs ++ [newChange])
                ) emptyDataChangeBatch (dcbChanges changeBatch)
          unless (L.null $ dcbChanges outputChangeBatch) $
            emitChangeBatch shard node outputChangeBatch
        TimestampIncSpec _ -> do
          let outputChangeBatch = L.foldl
                (\acc change -> do
                    let outputTs  = incCoord (dcTimestamp change)
                        newChange = DataChange
                          { dcRow = dcRow change
                          , dcTimestamp = outputTs
                          , dcDiff = dcDiff change
                          }
                    updateDataChangeBatch acc (\xs -> xs ++ [newChange])
                ) emptyDataChangeBatch (dcbChanges changeBatch)
          unless (L.null $ dcbChanges outputChangeBatch) $
            emitChangeBatch shard node outputChangeBatch
        TimestampPopSpec _ -> do
          let outputChangeBatch = L.foldl
                (\acc change -> do
                    let outputTs  = popCoord (dcTimestamp change)
                        newChange = DataChange
                          { dcRow = dcRow change
                          , dcTimestamp = outputTs
                          , dcDiff = dcDiff change
                          }
                    updateDataChangeBatch acc (\xs -> xs ++ [newChange])
                ) emptyDataChangeBatch (dcbChanges changeBatch)
          unless (L.null $ dcbChanges outputChangeBatch) $
            emitChangeBatch shard node outputChangeBatch
        UnionSpec _ _ -> do
          unless (L.null $ dcbChanges changeBatch) $
            emitChangeBatch shard node changeBatch
        DistinctSpec _ -> do
          let (DistinctState index_m pendingCorrections_m) = shardNodeStates' HM.! nodeId node
          mapM_ (\change -> do
                    pendingCorrections <- readTVarIO pendingCorrections_m
                    let key = dcRow change
                        corrExists = HM.lookup key pendingCorrections
                    let timestamps = case corrExists of
                                       Nothing  -> Set.empty
                                       Just tss -> tss
                    when (isNothing corrExists) $
                      atomically $ modifyTVar pendingCorrections_m (HM.insert key Set.empty)
                    case Set.member (dcTimestamp change) timestamps of
                      True  -> return ()
                      False -> do
                        atomically $
                          modifyTVar pendingCorrections_m (HM.adjust (Set.insert (dcTimestamp change)) key)
                        applyFrontierChange shard node (dcTimestamp change) 1
                        mapM_ (\entry -> do
                                  curPendingCorrections <- readTVarIO pendingCorrections_m
                                  let lub = leastUpperBound (dcTimestamp change) entry
                                  let curTimestamps = (HM.!) curPendingCorrections key
                                  case Set.member lub curTimestamps of
                                    True  -> return ()
                                    False -> do
                                      atomically $
                                        modifyTVar pendingCorrections_m (HM.adjust (Set.insert lub) key)
                                      void $ applyFrontierChange shard node lub 1
                                  ) (Set.insert (dcTimestamp change) timestamps)
                ) (dcbChanges changeBatch)
        ReduceSpec _ _ keygen _ -> do
          let (ReduceState _ pendingCorrections_m) = shardNodeStates' HM.! nodeId node
          mapM_ (\change -> do
                    pendingCorrections <- readTVarIO pendingCorrections_m
                    let key = keygen (dcRow change)
                        corrExists = HM.lookup key pendingCorrections
                    let timestamps = case corrExists of
                                       Nothing  -> Set.empty
                                       Just tss -> tss
                    when (isNothing corrExists) $
                      atomically $ modifyTVar pendingCorrections_m (HM.insert key Set.empty)
                    case Set.member (dcTimestamp change) timestamps of
                      True  -> return ()
                      False -> do
                        atomically $
                          modifyTVar pendingCorrections_m (HM.adjust (Set.insert (dcTimestamp change)) key)
                        applyFrontierChange shard node (dcTimestamp change) 1
                        mapM_ (\entry -> do
                                  curPendingCorrections <- readTVarIO pendingCorrections_m
                                  let lub = leastUpperBound (dcTimestamp change) entry
                                  let curTimestamps = (HM.!) curPendingCorrections key
                                  case Set.member lub curTimestamps of
                                    True  -> return ()
                                    False -> do
                                      atomically $
                                        modifyTVar pendingCorrections_m (HM.adjust (Set.insert lub) key)
                                      void $ applyFrontierChange shard node lub 1
                                  ) (Set.insert (dcTimestamp change) timestamps)
                ) (dcbChanges changeBatch)

queueFrontierChange :: (Hashable a, Ord a, Show a) => Shard a -> NodeInput -> Timestamp a -> Int -> IO ()
queueFrontierChange Shard{..} nodeInput@NodeInput{..} ts diff = do
  assert (diff /= 0) (return ())
  shardUnprocessedFrontierUpdates' <- readMVar shardUnprocessedFrontierUpdates
  let nodeSpec = graphNodeSpecs shardGraph HM.! nodeId nodeInputNode
      inputNode = (V.!) (getInputsFromSpec nodeSpec) nodeInputIndex
  let thisSubgraphs = graphNodeSubgraphs shardGraph HM.! nodeId nodeInputNode
      pointstamp = Pointstamp
        { pointstampNodeInput = nodeInput
        , pointstampSubgraphs = thisSubgraphs
        , pointstampTimestamp = ts
        }
  case HM.lookup pointstamp shardUnprocessedFrontierUpdates' of
    Nothing ->
      modifyMVar_ shardUnprocessedFrontierUpdates (return . HM.insert pointstamp diff)
    Just n  ->
      if n + diff == 0 then
        modifyMVar_ shardUnprocessedFrontierUpdates (return . HM.delete pointstamp)
      else
        modifyMVar_ shardUnprocessedFrontierUpdates (return . HM.adjust (+ diff) pointstamp)

-- True:  Updated
-- False: Not updated
applyFrontierChange :: (Hashable a, Ord a, Show a) => Shard a -> Node -> Timestamp a -> Int -> IO Bool
applyFrontierChange shard@Shard{..} node ts diff = do
  shardNodeFrontiers' <- readMVar shardNodeFrontiers
  case HM.lookup (nodeId node) shardNodeFrontiers' of
    Nothing  -> error $ "No matching node found: " <> show (nodeId node)
    Just tsf -> do
      let (newTsf, ftChanges) = updateTimestampsWithFrontier tsf ts diff
      modifyMVar_ shardNodeFrontiers (return . HM.insert (nodeId node) newTsf)
      mapM_ (\ftChange -> do
                mapM_ (\nodeInput ->
                         queueFrontierChange shard nodeInput
                           (frontierChangeTs ftChange) (frontierChangeDiff ftChange)
                      ) (graphDownstreamNodes shardGraph HM.! nodeId node)
            ) ftChanges
      if L.null ftChanges then return False else return True


processFrontierUpdates :: forall a. (Hashable a, Ord a, Show a) => Shard a -> IO ()
processFrontierUpdates shard@Shard{..} = do
  shardUnprocessedFrontierUpdates' <- readMVar shardUnprocessedFrontierUpdates
  updatedNodes <- go shardUnprocessedFrontierUpdates'
  mapM_ specialActions updatedNodes
  where
    -- process pointstamps in causal order to ensure termination
    go :: HM.HashMap (Pointstamp a) Int -> IO (Set Node) -- return: updatedNodes
    go unprocessedNow = if HM.null unprocessedNow then return Set.empty else do
      modifyMVar_ shardUnprocessedFrontierUpdates (return . HM.delete minKey)

      let outputTs = case graphNodeSpecs shardGraph HM.! nodeId node of
            TimestampPushSpec _ -> pushCoord inputTs
            TimestampIncSpec  _ -> incCoord inputTs
            TimestampPopSpec  _ -> popCoord inputTs
            _                   -> inputTs
      applyFrontierChange shard node outputTs diff

      loopResult <- go $ HM.delete minKey unprocessedNow
      return $ Set.insert node loopResult
      where
        minKey = L.minimum (HM.keys unprocessedNow)
        node = nodeInputNode (pointstampNodeInput minKey)
        inputTs = pointstampTimestamp minKey
        diff = unprocessedNow HM.! minKey

    specialActions :: Node -> IO ()
    specialActions node = do
      shardNodeStates' <- readMVar shardNodeStates
      let nodeSpec  = graphNodeSpecs shardGraph HM.! nodeId node
          nodeState = shardNodeStates' HM.! nodeId node
      case nodeState of
        IndexState index_m pendingChanges_m      -> do
          shardNodeFrontiers' <- readMVar shardNodeFrontiers
          pendingChanges <- readTVarIO pendingChanges_m
          let (IndexSpec inputNode) = nodeSpec
          let inputTsf = shardNodeFrontiers' HM.! nodeId inputNode
          newDataChangeBatch <-
            foldM (\curDataChangeBatch change -> do
                      case tsfFrontier inputTsf `causalCompare` dcTimestamp change of
                        PGT -> do
                          applyFrontierChange shard node (dcTimestamp change) (-1)
                          atomically $
                            modifyTVar pendingChanges_m (L.delete change)
                          return $ updateDataChangeBatch curDataChangeBatch (++ [change])
                        _   -> return curDataChangeBatch
                  ) emptyDataChangeBatch pendingChanges
          atomically $ modifyTVar index_m
            (\oldIndex -> addChangeBatchToIndex oldIndex newDataChangeBatch)
          unless (L.null $ dcbChanges newDataChangeBatch) $
            emitChangeBatch shard node newDataChangeBatch
        DistinctState index_m pendingCorrections_m -> do
          let inputNode = V.head $ getInputsFromSpec nodeSpec
          shardNodeFrontiers' <- readMVar shardNodeFrontiers
          let inputTsf = shardNodeFrontiers' HM.! nodeId inputNode
          shardNodeStates' <- readMVar shardNodeStates
          let inputIndex_m = getIndexFromState (shardNodeStates' HM.! nodeId inputNode)
          pendingCorrections <- readTVarIO pendingCorrections_m
          undefined
        ReduceState index_m pendingCorrections_m   -> do
          let inputNode = V.head $ getInputsFromSpec nodeSpec
          shardNodeFrontiers' <- readMVar shardNodeFrontiers
          let inputTsf = shardNodeFrontiers' HM.! nodeId inputNode
          shardNodeStates' <- readMVar shardNodeStates
          let inputIndex_m = getIndexFromState (shardNodeStates' HM.! nodeId inputNode)
          pendingCorrections <- readTVarIO pendingCorrections_m
          mapM_ (goPendingCorrection nodeSpec (tsfFrontier inputTsf) inputIndex_m index_m) (HM.toList pendingCorrections)
        _ -> return ()
      where
        goPendingCorrection :: NodeSpec -> Frontier a -> TVar (Index a) -> TVar (Index a) -> (Row, Set (Timestamp a)) -> IO ()
        goPendingCorrection (ReduceSpec _ initValue keygen (Reducer reducer)) inputFt inputIndex_m outputIndex_m (key, timestamps) = do
          (tssToCheck, ftChanges) <-
            foldM (\(curTssToCheck,curFtChanges) ts -> do
                      if inputFt `causalCompare` ts == PGT then do
                        let newFtChange = FrontierChange
                                          { frontierChangeTs   = ts
                                          , frontierChangeDiff = -1
                                          }
                        return (ts:curTssToCheck, curFtChanges ++ [newFtChange])
                        else return (curTssToCheck,curFtChanges)
                  ) ([],[]) timestamps
          let realTimestamps = L.foldl (flip Set.delete) timestamps tssToCheck
          mapM_ (\tsToCheck -> do
                    inputIndex  <- readTVarIO inputIndex_m
                    outputIndex <- readTVarIO outputIndex_m
                    let inputChanges  = getChangesForKey inputIndex  (\row -> keygen row == key)
                        outputChanges = getChangesForKey outputIndex (\row -> keygen row == key)
                    let inputBag = L.foldl (\acc DataChange{..} ->
                                              MultiSet.insertMany dcRow dcDiff acc)
                                   MultiSet.empty
                                   (L.filter (\change ->
                                                dcTimestamp change <.= tsToCheck)
                                     inputChanges)
                    let sortedInputs = L.sort $ MultiSet.toOccurList inputBag
                    let inputValue = L.foldl (\acc (x,_) -> reducer acc x) initValue sortedInputs
                    let outputChanges' =
                          L.map (\change -> change {dcDiff = - (dcDiff change)})
                            (L.filter (\change -> dcTimestamp change <.= tsToCheck) outputChanges)
                    let newOutput = DataChange (key <> inputValue) tsToCheck 1
                        outputChanges'' = outputChanges' ++ [newOutput]
                    let changeBatch = mkDataChangeBatch outputChanges''
                    atomically $
                      modifyTVar outputIndex_m (flip addChangeBatchToIndex changeBatch)
                    unless (L.null $ dcbChanges changeBatch) $
                      emitChangeBatch shard node changeBatch
                    mapM_ (\FrontierChange{..} -> applyFrontierChange shard node frontierChangeTs frontierChangeDiff) ftChanges
                ) (L.sort tssToCheck)

getOutputNodes :: Graph -> [Node]
getOutputNodes Graph{..} = L.map Node . HM.keys $
  HM.filterWithKey (\i spec -> case spec of
                                 OutputSpec _ -> True
                                 _            -> False
                   ) graphNodeSpecs

outputNodeNotEmpty :: Shard a -> Node -> IO Bool
outputNodeNotEmpty Shard{..} node = do
  shardNodeStates' <- readMVar shardNodeStates
  let (OutputState dcbs_m) = shardNodeStates' HM.! (nodeId node)
  dcbs <- readTVarIO dcbs_m
  return . not $ L.null dcbs

hasWork :: Shard a -> IO Bool
hasWork shard@Shard{..} = do
  shardUnprocessedChangeBatches' <- readMVar shardUnprocessedChangeBatches
  shardUnprocessedFrontierUpdates' <- readMVar shardUnprocessedFrontierUpdates
  return $
    not (L.null shardUnprocessedChangeBatches')    ||
    not (HM.null shardUnprocessedFrontierUpdates')

doWork :: (Hashable a, Ord a, Show a) => Shard a -> IO ()
doWork shard@Shard{..} = do
  shardUnprocessedChangeBatches' <- readMVar shardUnprocessedChangeBatches
  shardUnprocessedFrontierUpdates' <- readMVar shardUnprocessedFrontierUpdates
  shardNodeStates' <- readMVar shardNodeStates
  if not (L.null shardUnprocessedChangeBatches') then do
    print $ "=== Working (processChangeBatch)..."
    processChangeBatch shard else
    if not (L.null shardUnprocessedFrontierUpdates') then do
      print $ "=== Working (processFrontierUpdates)..."
      processFrontierUpdates shard else return ()

popOutput :: (Show a) => Shard a -> Node -> (DataChangeBatch a -> IO ()) -> IO ()
popOutput Shard{..} node action = do
  shardNodeStates' <- readMVar shardNodeStates
  let (OutputState dcbs_m) = shardNodeStates' HM.! nodeId node

  dcb' <- atomically $
    stateTVar dcbs_m (\xs -> case xs of
                               []      -> (Nothing, xs)
                               (x:xs') -> (Just x, xs'))
  case dcb' of
    Nothing  -> threadDelay 1000000
    Just dcb -> action dcb

run :: (Hashable a, Ord a, Show a) => Shard a -> IO ()
run shard = forever $ do
  work <- hasWork shard
  print $ "Loop: still has work?" <> show work
  if work then do
    doWork shard else do
    threadDelay 2000000
