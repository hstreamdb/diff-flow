{-# LANGUAGE OverloadedStrings #-}

module DiffFlow.TypesSpec where

import MyLib
import Test.Hspec
import Data.MultiSet (MultiSet)
import qualified Data.MultiSet as MultiSet
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Hashable (Hashable)
import Data.Aeson (Value (..))

spec :: Spec
spec = describe "TypesSpec" $ do
  timestampsWithFrontier
  dataChangeBatch


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
