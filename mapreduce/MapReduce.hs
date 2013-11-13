module MapReduce where

--import qualified Data.Map as Map
import qualified Data.Map as Map

shuffle :: Ord k => [(k, v)] -> Map.Map k [v]
shuffle kvs = Map.fromListWith (++) [(k, [v]) | (k, v) <- kvs]

idMapF :: (k, v) -> [(k, v)]
idMapF = return . id

idReduceF :: (k, [v]) -> [(k, v)]
idReduceF (k, vs) = [(k, v) | v <- vs]

-- mapReduce :: (Ord k, Ord k2, Ord k1) =>
--     (a -> [(k, v)])
--     -> ((k2, [v1]) -> [(k1, v2)])
--     -> ((k, [v]) -> [(k2, v1)])
--     -> [a]
--     -> [(k1, [v2])]

--  map_f :: (k1, v1) -> iterable((k2, v2))
--  reduce_f :: (k2, [v2]) -> iterable((k3, v3))
--  combine_f :: (k2, [v2]) -> iterable((k2, v2))

-- TODO: use HO fun to repeat application
mapReduce2 mapF reduceF combineF inputPairs =  Map.toList $ (shuffleReduceKVDict reduceF) $ shuffleDict
    where 
      -- combineDict :: Ord k2 => Map.Map k2 [v2]
      combineDict = shuffle $ (concatMap mapF) $ inputPairs
      reduceKeyValsDict reduceF = Map.foldlWithKey (\cps k vs -> reduceF k vs ++ cps) []
      shuffleReduceKVDict reduceF =  shuffle . (reduceKeyValsDict reduceF)
      shuffleDict = shuffleReduceKVDict combineF combineDict
      -- shuffleDict = shuffle $ Map.foldlWithKey (\cps k2 v2s -> combineF k2 v2s ++ cps) [] combineDict


-- foldlWithKey :: (a -> k -> b -> a) -> a -> Map k b -> a

--      shuffleDict = shuffle $ (concatMap combineF) $ Map.toList $ combineDict

mapReduce mapF reduceF combineF inputPairs =  Map.toList $ shuffle $ (concatMap reduceF) $ Map.toList $ shuffleDict
    where 
      combineDict = shuffle $ (concatMap mapF) $ inputPairs
      shuffleDict = shuffle $ (concatMap combineF) $ Map.toList $ combineDict


wordCount = mapReduce mapF reduceF reduceF
    where 
      mapF (_, text) = [(word, 1) | word <- words text]
      reduceF (word, counts) = [(word, foldl1 (+) counts)]

wordCount2 = mapReduce2 mapF reduceF reduceF
    where 
      mapF (_, text) = [(word, 1) | word <- words text]
      reduceF (word, counts) = [(word, foldl1 (+) counts)]
      
wcInput = [("Moon", "'Never' does not exist for the human mind… only 'Not yet.'"),
            ("Spooner", "You must be the dumbest, smart person in the world. And you must be the dumbest, dumb person in the world."),
            ("Blake", "Easy! Take it easy! I hate personal violence, especially when I’m the person.")]

test = do 
        print (wordCount wcInput)
        print (wordCount2 wcInput)

--main :: IO()
--main = do
--    where
--      wc_input = [("Moon", "'Never' does not exist for the human mind… only 'Not yet.'"),
--                ("Spooner", "You must be the dumbest, smart person in the world. And you must be the dumbest, dumb person in the world."),
--                ("Blake", "Easy! Take it easy! I hate personal violence, especially when I’m the person.")]
