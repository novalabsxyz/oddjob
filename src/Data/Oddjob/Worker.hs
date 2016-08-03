{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE RankNTypes #-}

module Data.Oddjob.Worker
    ( WorkerService(..)
    , WorkerServiceState(..)
    , WorkerServiceEvent(..)
    , WorkerFn
    , EventHandler
    , Jobs
    , startWorkerService
    , stopWorkerService
    , setJobs
    , setJobsSTM
    , getState
    ) where

import           Prelude hiding (mapM_)
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad (void, forever, when, unless)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.State.Strict (MonadState, StateT, evalStateT, get)
import           Data.Foldable (mapM_)
import           Data.Maybe (fromMaybe)
import qualified Data.Map.Strict as Map
import           Data.Map.Strict ((!))
import           Data.Set (Set, (\\))
import qualified Data.Set as Set
import           Data.Time (DiffTime, picosecondsToDiffTime)
import           Lens.Micro (Lens', (^.), lens)
import           Lens.Micro.Mtl ((%=), use)
import           System.Clock (Clock(Monotonic), TimeSpec, getTime,
                               diffTimeSpec, timeSpecAsNanoSecs)

type WorkerHandle = Async ()

type WorkerMap b = Map.Map b (TimeSpec, WorkerHandle)

type Jobs b = Set b

type WorkerFn b = b -> IO ()

data WorkerServiceState b
    = WorkerServiceState [(b, DiffTime)]
    deriving (Show, Functor)

data WorkerServiceEvent b
    = JobStarted [b]
    | JobCancelled [(b, DiffTime)]
    | JobCrashed b DiffTime SomeException
    deriving (Show, Functor)

type EventHandler b = WorkerServiceEvent b -> IO ()

data WorkerService b = WorkerService
    { _asyncHandle :: Async ()
    , _inputVar :: TMVar (Jobs b)
    , _stateVar :: TMVar (TMVar (WorkerServiceState b))
    }

asyncHandle :: Lens' (WorkerService b) (Async ())
asyncHandle = lens _asyncHandle (\s n -> s { _asyncHandle = n })

inputVar :: Lens' (WorkerService b) (TMVar (Jobs b))
inputVar = lens _inputVar (\s n -> s { _inputVar = n })

stateVar :: Lens' (WorkerService b) (TMVar (TMVar (WorkerServiceState b)))
stateVar = lens _stateVar (\s n -> s { _stateVar = n })


data WorkerState b = WorkerState
    { _workerMap :: WorkerMap b
    , _updateVar:: TMVar (Jobs b)
    , _stateRequest :: TMVar (TMVar (WorkerServiceState b))
    , _diedQueue :: TBQueue (SomeException, b)
    , _runJob :: WorkerFn b
    , _self :: Async ()
    , _eventHandler :: EventHandler b
    }

data WakeupReason b
    = SetJobs (Jobs b)
    | Crashed SomeException b
    | StateRequest (TMVar (WorkerServiceState b))

workerMap :: Lens' (WorkerState b) (WorkerMap b)
workerMap = lens _workerMap (\s n -> s { _workerMap = n })

updateVar :: Lens' (WorkerState b) (TMVar (Jobs b))
updateVar = lens _updateVar (\s n -> s { _updateVar = n })

diedQueue :: Lens' (WorkerState b) (TBQueue (SomeException, b))
diedQueue = lens _diedQueue (\s n -> s { _diedQueue = n })

runJob :: Lens' (WorkerState b) (b -> IO ())
runJob = lens _runJob (\s n -> s { _runJob = n })

self :: Lens' (WorkerState b) (Async ())
self = lens _self (\s n -> s { _self = n })

eventHandler :: Lens' (WorkerState b) (EventHandler b)
eventHandler = lens _eventHandler (\s n -> s { _eventHandler = n })

stateRequest :: Lens' (WorkerState b) (TMVar (TMVar (WorkerServiceState b)))
stateRequest = lens _stateRequest (\s n -> s { _stateRequest = n })

startWorkerService
    :: Ord b
    => WorkerFn b
    -> Maybe (EventHandler b)
    -> IO (WorkerService b)
startWorkerService jobFn mEventHandler = do
    let eHandler = fromMaybe (const (return ())) mEventHandler
    updateQ <- newEmptyTMVarIO
    stateReqQ <- newEmptyTMVarIO
    diedQ <- newTBQueueIO 10
    controller <- asyncWithReferenceToSelf $ \me ->
                    let initial = WorkerState Map.empty updateQ stateReqQ diedQ jobFn me eHandler
                    in
                    runWorkerService workerServiceLoop initial
    return (WorkerService controller updateQ stateReqQ)

stopWorkerService :: WorkerService a -> IO ()
stopWorkerService service = cancel (service ^. asyncHandle)

setJobs :: Ord b => WorkerService b -> Jobs b -> IO ()
setJobs service jobs = atomically (setJobsSTM service jobs)

setJobsSTM :: Ord b => WorkerService b -> Jobs b -> STM ()
setJobsSTM service jobs = putTMVar (service ^. inputVar) jobs

getState :: Ord b => WorkerService b -> IO (WorkerServiceState b)
getState service = do
    replyVar <- newEmptyTMVarIO
    atomically (putTMVar (service ^. stateVar) replyVar)
    atomically (takeTMVar replyVar)

newtype WS b a = WS
    { _unWorkerState :: StateT (WorkerState b) IO a }
    deriving ( Functor, Applicative, Monad, MonadIO,
               MonadState (WorkerState b)
             )

runWorkerService :: WS b a -> WorkerState b -> IO a
runWorkerService s = evalStateT (_unWorkerState s)

jobStatus :: Ord b => TimeSpec -> b -> WorkerMap b -> DiffTime
jobStatus now job wMap =
    let (workerStartTime, _) = wMap ! job
        timeDiff = diffTimeSpec now workerStartTime
    in timeSpecToDiffTime timeDiff

controlJob :: Ord b => b -> WS b ()
controlJob job = do
    jobFn <- use runJob
    diedQ <- use diedQueue
    workerHandle <- liftIO (asyncFinally
                        (jobFn job)
                        (\e ->
                            atomically (writeTBQueue diedQ (e, job))))
    me <- use self
    workerStartTime <- liftIO (getTime Monotonic)
    liftIO (linkChildToParent me workerHandle)
    workerMap %= Map.insert job (workerStartTime, workerHandle)

cancelJob :: Ord b => b -> WS b ()
cancelJob job = do
    (_, workerHandle) <- (! job) <$> use workerMap
    liftIO (cancel workerHandle)
    workerMap %= Map.delete job

updateRunningState :: Ord b => Jobs b -> WS b ()
updateRunningState needRunning = do
    wMap <- use workerMap
    eHandler <- use eventHandler
    let currentlyRunning = Set.fromList (Map.keys wMap)
        noLongerNeeded = currentlyRunning \\ needRunning
        notRunning = needRunning \\ currentlyRunning

    void $ liftIO $ do
        updateTime <- getTime Monotonic
        let jobStatusTuple j = (j, jobStatus updateTime j wMap)

        -- Call the event handler with the cancelled jobs
        let canceling = fmap jobStatusTuple (Set.toList noLongerNeeded)
        unless (null canceling)
            (eHandler (JobCancelled canceling))

        unless (Set.null notRunning)
            (eHandler (JobStarted (Set.toList notRunning)))

    mapM_ cancelJob noLongerNeeded

    mapM_ controlJob notRunning

workerServiceLoop :: Ord b => WS b ()
workerServiceLoop = forever $ do
    reasonToWakeUp <- get >>= liftIO . atomically . wakeupSTM
    case reasonToWakeUp of
        (SetJobs needRunning) ->
            updateRunningState needRunning
        (Crashed exception job) -> do
            wMap <- use workerMap
            eHandler <- use eventHandler

            -- Only restart the worker if we're actually supposed to be
            -- running it.
            when (Map.member job wMap) $ do
                -- Notify the event handler
                liftIO $ do
                    updateTime <- getTime Monotonic
                    let diffTime = jobStatus updateTime job wMap
                    eHandler (JobCrashed job diffTime exception)

                -- this results in an unneeded Async.cancel, but it keeps the code
                -- nice and clean, and it's innocuous
                cancelJob job
                controlJob job
        (StateRequest replyQ) -> do
            wMap <- use workerMap
            now <- liftIO (getTime Monotonic)
            let statuses = fmap (\j -> (j, jobStatus now j wMap)) (Map.keys wMap)
            liftIO (atomically (putTMVar replyQ (WorkerServiceState statuses)))

wakeupSTM :: WorkerState b -> STM (WakeupReason b)
wakeupSTM apState =
    (SetJobs <$> needRunningSTM (apState ^. updateVar))
        `orElse`
    (StateRequest <$> stateRequestSTM (apState ^. stateRequest))
        `orElse`
    (uncurry Crashed <$> stoppedWithExceptionSTM (apState ^. diedQueue))

needRunningSTM :: TMVar (Jobs b) -> STM (Jobs b)
needRunningSTM = takeTMVar

stateRequestSTM :: TMVar (TMVar (WorkerServiceState b))
                -> STM (TMVar (WorkerServiceState b))
stateRequestSTM = takeTMVar

stoppedWithExceptionSTM :: TBQueue (SomeException, b) -> STM (SomeException, b)
stoppedWithExceptionSTM = readTBQueue

-- Time Helpers **************************************************************
------------------------------------------------------------------------------

timeSpecToDiffTime :: TimeSpec -> DiffTime
timeSpecToDiffTime ts = picosecondsToDiffTime (timeSpecAsNanoSecs ts * 1000)

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

asyncFinally :: IO a -> (SomeException -> IO b) -> IO (Async a)
asyncFinally action handler =
    let finallyE io what = io `catch` \e ->
                                what e >> throwIO (e :: SomeException)
    in
    mask $ \restore ->
        async (restore action `finallyE` handler)

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
