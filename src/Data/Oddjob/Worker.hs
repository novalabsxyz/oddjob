{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}

module Data.Oddjob.Worker
    ( WorkerService
    , Jobs
    , startWorkerService
    , stopWorkerService
    , setJobs
    ) where

import           Prelude hiding (mapM_)
import           Control.Applicative (Applicative, (<$>))
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad (void, forever, when)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.State.Strict (MonadState, StateT, evalStateT, get)
import           Data.Foldable (mapM_)
import           Data.Monoid ((<>))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict ((!))
import           Data.Set (Set, (\\))
import qualified Data.Set as Set
import           Lens.Micro (Lens', (^.), lens)
import           Lens.Micro.Mtl ((%=), use)

type WorkerHandle = Async ()

type WorkerMap b = Map.Map b WorkerHandle

type Jobs b = Set b

data WorkerService b = WorkerService
    { _asyncHandle :: Async ()
    , _inputVar:: TMVar (Jobs b)
    } deriving (Eq)

asyncHandle :: Lens' (WorkerService b) (Async ())
asyncHandle = lens _asyncHandle (\s n -> s { _asyncHandle = n })

inputVar :: Lens' (WorkerService b) (TMVar (Jobs b))
inputVar = lens _inputVar (\s n -> s { _inputVar = n })


data WorkerState b = WorkerState
    { _workerMap :: WorkerMap b
    , _updateVar:: TMVar (Jobs b)
    , _diedQueue :: TBQueue b
    , _runJob :: b -> IO ()
    , _self :: Async ()
    }

workerMap :: Lens' (WorkerState b) (WorkerMap b)
workerMap = lens _workerMap (\s n -> s { _workerMap = n })

updateVar :: Lens' (WorkerState b) (TMVar (Jobs b))
updateVar = lens _updateVar (\s n -> s { _updateVar = n })

diedQueue :: Lens' (WorkerState b) (TBQueue b)
diedQueue = lens _diedQueue (\s n -> s { _diedQueue = n })

runJob :: Lens' (WorkerState b) (b -> IO ())
runJob = lens _runJob (\s n -> s { _runJob = n })

self :: Lens' (WorkerState b) (Async ())
self = lens _self (\s n -> s { _self = n })

startWorkerService :: Ord b => (b -> IO ()) -> IO (WorkerService b)
startWorkerService jobFn = do
    updateQ <- newEmptyTMVarIO
    diedQ <- newTBQueueIO 10
    controller <- asyncWithReferenceToSelf $ \me ->
                    let initial = WorkerState Map.empty updateQ diedQ jobFn me
                    in
                    runWorkerService workerServiceLoop initial
    return (WorkerService controller updateQ)

stopWorkerService :: WorkerService a -> IO ()
stopWorkerService service = cancel (service ^. asyncHandle)

setJobs :: Ord b => WorkerService b -> Jobs b -> IO ()
setJobs service jobs = atomically (putTMVar (service ^. inputVar) jobs)

newtype WS b a = WS
    { _unWorkerState :: StateT (WorkerState b) IO a }
    deriving ( Functor, Applicative, Monad, MonadIO,
               MonadState (WorkerState b)
             )

runWorkerService :: WS b a -> WorkerState b -> IO a
runWorkerService s = evalStateT (_unWorkerState s)

controlJob :: Ord b => b -> WS b ()
controlJob job = do
    jobFn <- use runJob
    diedQ <- use diedQueue
    worker <- liftIO (asyncFinally
                        (jobFn job)
                        (atomically (writeTBQueue diedQ job)))
    me <- use self
    liftIO (linkChildToParent me worker)
    workerMap %= Map.insert job worker

cancelJob :: Ord b => b -> WS b ()
cancelJob job = do
    workerHandle <- (! job) <$> use workerMap
    liftIO (cancel workerHandle)
    workerMap %= Map.delete job

updateRunningState :: Ord b => Jobs b -> WS b ()
updateRunningState needRunning = do
    wMap <- use workerMap
    let currentlyRunning = Set.fromList (Map.keys wMap)
        noLongerNeeded = currentlyRunning \\ needRunning
        notRunning = needRunning \\ currentlyRunning
    mapM_ cancelJob noLongerNeeded
    mapM_ controlJob notRunning

workerServiceLoop :: Ord b => WS b ()
workerServiceLoop = forever $ do
    reasonToWakeUp <- get >>= liftIO . atomically . wakeupSTM
    case reasonToWakeUp of
        (Left needRunning) ->
            updateRunningState needRunning
        (Right job) -> do
            wMap <- use workerMap
            -- Only restart the worker if we're actually suppoed to be
            -- running it.
            when (Map.member job wMap) $ do
                liftIO (putStrLn "restarting stopped process")
                -- this results in an unneeded Async.cancel, but it keeps the code
                -- nice and clean, and it's innocuous
                cancelJob job
                controlJob job

wakeupSTM :: WorkerState b -> STM (Either (Jobs b) b)
wakeupSTM apState =
    -- TODO should we swap the order of these?
    (Left <$> needRunningSTM (apState ^. updateVar))
        `orElse`
    (Right <$> stoppedWithExceptionSTM (apState ^. diedQueue))

needRunningSTM :: TMVar (Jobs b) -> STM (Jobs b)
needRunningSTM = takeTMVar

stoppedWithExceptionSTM :: TBQueue b -> STM b
stoppedWithExceptionSTM = readTBQueue

-- Things Reid wishes were in Control.Concurrent.Async ***********************
------------------------------------------------------------------------------

asyncWithReferenceToSelf :: (Async a -> IO a) -> IO (Async a)
asyncWithReferenceToSelf action = do
    var <- newEmptyMVar
    a <- async (takeMVar var >>= action)
    putMVar var a
    return a

linkChildToParent :: Async a -> Async b -> IO ()
linkChildToParent parent child =
    void $ forkRepeat $ do
        r <- waitCatch parent
        case r of
            (Left e) -> cancelWith child e
            _ -> return ()

asyncFinally :: IO a -> IO b -> IO (Async a)
asyncFinally action handler =
    mask $ \restore ->
        async (restore action `finally` handler)

-- From Control.Concurrent.Async internals
--
-- | Fork a thread that runs the supplied action, and if it raises an
-- exception, re-runs the action.  The thread terminates only when the
-- action runs to completion without raising an exception.
forkRepeat :: IO a -> IO ThreadId
forkRepeat action =
  mask $ \restore ->
    let go = do r <- tryAll (restore action)
                case r of
                  Left _ -> go
                  _      -> return ()
    in forkIO go

-- From Control.Concurrent.Async internals
--
tryAll :: IO a -> IO (Either SomeException a)
tryAll = try
