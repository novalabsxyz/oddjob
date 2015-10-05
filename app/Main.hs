module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Monad (forever, forM_)
import Data.Monoid ((<>))
import Data.Set (fromList, toList)

import Data.Oddjob.Worker

lockedPutStrLn :: MVar () -> String -> IO ()
lockedPutStrLn lock str =
    withMVar lock (\_ -> putStrLn str)

worker :: MVar () -> Integer -> IO ()
worker lock job = do
    lockedPutStrLn lock ("Starting job: " <> show job)
    forever $ do
        lockedPutStrLn lock ("Worker: " <> show job)
        threadDelay halfSecond

halfSecond :: Int
halfSecond = 500000

oneSecond :: Int
oneSecond = 1000000

main :: IO ()
main = do
    lock <- newMVar ()
    service <- startWorkerService (worker lock)
    let jobsOverTime = map fromList [ [1..2]
                                    , [1..3]
                                    , [2..3]
                                    , [1..4]
                                    ]
    forM_ jobsOverTime $ \j -> do
        lockedPutStrLn lock ("Settings new jobs: " <> show (toList j))
        setJobs service j
        threadDelay oneSecond
    threadDelay (2 * oneSecond)
