{-# LANGUAGE OverloadedStrings #-}

module DiffFlow.TypesSpec where

import Types
import Test.Hspec
import Data.MultiSet (MultiSet)
import qualified Data.MultiSet as MultiSet
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Hashable (Hashable)
import Data.Aeson (Value (..))
import qualified Data.List as L

spec :: Spec
spec = describe "TypesSpec" $ do
  timestampsWithFrontier
  dataChangeBatch
  index


updateTimestampsWithFrontierChecker :: (Ord a, Show a)
                                    => MultiSet (Timestamp a) -- initial timestamps
                                    -> (Timestamp a, Int)     -- update
                                    -> Frontier a             -- expected frontier
                                    -> [FrontierChange a]     -- expected frontier changes
                                    -> Bool
updateTimestampsWithFrontierChecker tss (ts,diff) expectedFrontier expectedChanges =
  ((tsfFrontier $ fst actualTsf) == expectedFrontier) && (snd actualTsf == expectedChanges)
  where
    emptyTsf = TimestampsWithFrontier MultiSet.empty Set.empty
    initTsf  = MultiSet.foldOccur (\x n acc -> acc ->> (x,n)) emptyTsf tss
    actualTsf = updateTimestampsWithFrontier initTsf ts diff


mkDataChangeBatchChecker :: (Hashable a, Ord a, Show a)
                         => [DataChange a] -- input data changes
                         -> [DataChange a] -- expected data changes
                         -> Bool
mkDataChangeBatchChecker changes expectedChanges =
  dcbChanges dataChangeBatch == expectedChanges
  where dataChangeBatch = mkDataChangeBatch changes

addChangeBatchToIndexChecker :: (Hashable a, Ord a, Show a)
                             => [DataChangeBatch a]
                             -> Index a
                             -> Bool
addChangeBatchToIndexChecker batches expectedIndex = expectedIndex == actualIndex
  where actualIndex =
          L.foldl addChangeBatchToIndex (Index []) batches

timestampsWithFrontier :: Spec
timestampsWithFrontier = describe "TimestampsWithFrontier" $ do
  it "update TimestampsWithFrontier" $ do
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 2 [0], Timestamp 0 [1]])
      (Timestamp 1 [0], 1)
      (Set.fromList [Timestamp 0 [1], Timestamp 1 [0]])
      [FrontierChange (Timestamp 1 [0]) 1, FrontierChange (Timestamp 2 [0]) (-1)]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0]])
      (Timestamp 1 [1], 1)
      (Set.fromList [Timestamp 0 [0]])
      []
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 1 [1]])
      (Timestamp 0 [0], 1)
      (Set.fromList [Timestamp 0 [0]])
      [FrontierChange (Timestamp 0 [0]) 1, FrontierChange (Timestamp 1 [1]) (-1)]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0], Timestamp 1 [1]])
      (Timestamp 0 [0], -1)
      (Set.fromList [Timestamp 1 [1]])
      [FrontierChange (Timestamp 0 [0]) (-1), FrontierChange (Timestamp 1 [1]) 1]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0], Timestamp 0 [0]])
      (Timestamp 0 [0], -1)
      (Set.fromList [Timestamp 0 [0]])
      []
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0]])
      (Timestamp 0 [0], -1)
      (Set.fromList [])
      [FrontierChange (Timestamp 0 [0]) (-1)]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0]])
      (Timestamp 0 [0], 1)
      (Set.fromList [Timestamp 0 [0]])
      []
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [])
      (Timestamp 0 [0], 1)
      (Set.fromList [Timestamp 0 [0]])
      [FrontierChange (Timestamp 0 [0]) 1]
      `shouldBe` True

dataChangeBatch :: Spec
dataChangeBatch = describe "DataChangeBatch" $ do
  it "make DataChangeBatch" $ do
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 0
      , DataChange [String "a"] (Timestamp  0         []) 0
      , DataChange [String "a"] (Timestamp  0         []) 1
      ]
      [ DataChange [String "a"] (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 0
      , DataChange [String "a"] (Timestamp  0         []) 0
      , DataChange [String "a"] (Timestamp  0         []) 0
      ]
      []
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
      , DataChange [String "a"] (Timestamp  0         []) (-1)
      , DataChange [String "a"] (Timestamp  0         []) 1
      ]
      [ DataChange [String "a"] (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
      , DataChange [String "b"] (Timestamp  0         []) 1
      , DataChange [String "a"] (Timestamp  0         []) 1
      , DataChange [String "b"] (Timestamp  0         []) (-1)
      ]
      [ DataChange [String "a"] (Timestamp  0         []) 2]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
      , DataChange [String "b"] (Timestamp  0         []) 1
      , DataChange [String "a"] (Timestamp  0         []) (-1)
      , DataChange [String "b"] (Timestamp  0         []) (-1)
      ]
      []
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
      , DataChange [String "b"] (Timestamp  0         []) 1
      , DataChange [String "a"] (Timestamp  0         []) (-1)
      ]
      [ DataChange [String "b"] (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
      ]
      [ DataChange [String "a"] (Timestamp  0         []) 1]
      `shouldBe` True


index :: Spec
index = describe "Index" $ do
  it "add DataChangeBatch to Index" $ do
    addChangeBatchToIndexChecker
      [mkDataChangeBatch [DataChange [String "a"] (Timestamp (0 :: Int) []) 1]]
      (Index [mkDataChangeBatch [DataChange [String "a"] (Timestamp 0 []) 1]])
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [DataChange [String "a"] (Timestamp (0 :: Int) []) 1]
      , mkDataChangeBatch [DataChange [String "a"] (Timestamp (0 :: Int) []) 1]
      ]
      (Index [mkDataChangeBatch [DataChange [String "a"] (Timestamp (0 :: Int) []) 2]])
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "b"] (Timestamp (0 :: Int) []) 1
                          ]
      , mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1]
      ]
      (Index [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "b"] (Timestamp (0 :: Int) []) 1
                                 ]
             , mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1]
             ]
      )
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "b"] (Timestamp (0 :: Int) []) 1
                          ]
      , mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "b"] (Timestamp (0 :: Int) []) (-1)
                          ]
      ]
      (Index [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 2]
             ]
      )
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "b"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "c"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "d"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "e"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "f"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "g"] (Timestamp (0 :: Int) []) 1 ]
      ]
      (Index [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "b"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "c"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "d"] (Timestamp (0 :: Int) []) 1
                                 ]
             , mkDataChangeBatch [ DataChange [String "e"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "f"] (Timestamp (0 :: Int) []) 1
                                 ]
             , mkDataChangeBatch [ DataChange [String "g"] (Timestamp (0 :: Int) []) 1 ]
             ]
      )
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "b"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "c"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "d"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "e"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "f"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "g"] (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange [String "h"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "i"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "j"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "k"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "l"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "m"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "n"] (Timestamp (0 :: Int) []) 1
                          , DataChange [String "o"] (Timestamp (0 :: Int) []) 1
                          ]
      ]
      (Index [ mkDataChangeBatch [ DataChange [String "a"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "b"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "c"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "d"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "e"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "f"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "g"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "h"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "i"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "j"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "k"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "l"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "m"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "n"] (Timestamp (0 :: Int) []) 1
                                 , DataChange [String "o"] (Timestamp (0 :: Int) []) 1
                                 ]
             ]
      )
      `shouldBe` True
