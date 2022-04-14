module DiffFlow.TypesSpec where

import MyLib
import Test.Hspec
import Data.MultiSet (MultiSet)
import qualified Data.MultiSet as MultiSet
import Data.Set (Set)
import qualified Data.Set as Set

spec :: Spec
spec = describe "TypesSpec" $ do
  timestampsWithFrontier



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
