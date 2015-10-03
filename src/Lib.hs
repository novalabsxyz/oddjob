{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}

module Lib
    ( AlertService
    , Jobs
    , startAlertService
    , stopAlertService
    , setJobs
    ) where

import           Prelude hiding (mapM_)
import           Control.Applicative (Applicative, (<$>))
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad (void, forever)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.State.Strict (MonadState, StateT, evalStateT, get, put)
import           Data.Foldable (mapM_)
import           Data.Monoid ((<>))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict ((!))
import           Data.Set (Set, (\\))
import qualified Data.Set as Set
import           Lens.Micro (Getting, Lens', (^.), lens, over)

use :: MonadState s m => Getting a s a -> m a
use l = do
    s <- get
    return (s ^. l)

(%=) :: MonadState s m => Lens' s a -> (a -> a) -> m ()
l %= f = do
    s <- get
    put (over l f s)

type WorkerHandle = Async ()

type WorkerMap b = Map.Map b WorkerHandle

type Jobs b = Set b

data AlertService b = AlertService
    { _asyncHandle :: Async ()
    , _inputVar:: TMVar (Jobs b)
    } deriving (Eq)

asyncHandle :: Lens' (AlertService b) (Async ())
asyncHandle = lens _asyncHandle (\s n -> s { _asyncHandle = n })

inputVar :: Lens' (AlertService b) (TMVar (Jobs b))
inputVar = lens _inputVar (\s n -> s { _inputVar = n })


data AlertPersistenceState b = AlertPersistenceState
    { _workerMap :: WorkerMap b
    , _updateVar:: TMVar (Jobs b)
    , _diedQueue :: TBQueue b
    , _runJob :: b -> IO ()
    , _self :: Async ()
    }

workerMap :: Lens' (AlertPersistenceState b) (WorkerMap b)
workerMap = lens _workerMap (\s n -> s { _workerMap = n })

updateVar :: Lens' (AlertPersistenceState b) (TMVar (Jobs b))
updateVar = lens _updateVar (\s n -> s { _updateVar = n })

diedQueue :: Lens' (AlertPersistenceState b) (TBQueue b)
diedQueue = lens _diedQueue (\s n -> s { _diedQueue = n })

runJob :: Lens' (AlertPersistenceState b) (b -> IO ())
runJob = lens _runJob (\s n -> s { _runJob = n })

self :: Lens' (AlertPersistenceState b) (Async ())
self = lens _self (\s n -> s { _self = n })

startAlertService :: Ord b => (b -> IO ()) -> IO (AlertService b)
startAlertService jobFn = do
    updateQ <- newEmptyTMVarIO
    diedQ <- newTBQueueIO 10
    controller <- asyncWithReferenceToSelf $ \me ->
                    let initial = AlertPersistenceState Map.empty updateQ diedQ jobFn me
                    in
                    runAlertPersistence betterSTMImplementation initial
    return (AlertService controller updateQ)

stopAlertService :: AlertService a -> IO ()
stopAlertService service = cancel (service ^. asyncHandle)

setJobs :: Ord b => AlertService b -> Jobs b -> IO ()
setJobs service jobs = atomically (putTMVar (service ^. inputVar) jobs)

newtype AlertPersistence b a = AlertPersistence
    { _unAlertPersistenceState :: StateT (AlertPersistenceState b) IO a }
    deriving ( Functor, Applicative, Monad, MonadIO,
               MonadState (AlertPersistenceState b)
             )

runAlertPersistence :: AlertPersistence b a -> AlertPersistenceState b -> IO a
runAlertPersistence s = evalStateT (_unAlertPersistenceState s)

controlAlertRule :: Ord b => b -> AlertPersistence b ()
controlAlertRule job = do
    jobFn <- use runJob
    diedQ <- use diedQueue
    worker <- liftIO (asyncFinally
                        (jobFn job)
                        (atomically (writeTBQueue diedQ job)))
    me <- use self
    liftIO (linkChildToParent me worker)
    workerMap %= Map.insert job worker

cancelAlertRule :: Ord b => b -> AlertPersistence b ()
cancelAlertRule job = do
    workerHandle <- (! job) <$> use workerMap
    liftIO (cancel workerHandle)
    workerMap %= Map.delete job

updateRunningState :: Ord b => Jobs b -> AlertPersistence b ()
updateRunningState needRunning = do
    wMap <- use workerMap
    let currentlyRunning = Set.fromList (Map.keys wMap)
        noLongerNeeded = currentlyRunning \\ needRunning
        notRunning = needRunning \\ currentlyRunning
    liftIO (putStrLn ("cancelling: " <> show (Set.size noLongerNeeded)))
    mapM_ cancelAlertRule noLongerNeeded
    liftIO (putStrLn ("starting: " <> show (Set.size notRunning)))
    mapM_ controlAlertRule notRunning

betterSTMImplementation :: Ord b => AlertPersistence b ()
betterSTMImplementation = forever $ do
    reasonToWakeUp <- get >>= liftIO . atomically . alertOnAnything
    case reasonToWakeUp of
        (Left needRunning) ->
            updateRunningState needRunning
        (Right job) -> do
            liftIO (putStrLn "restarting stopped process")
            -- this results in an unneeded Async.cancel, but it keeps the code
            -- nice and clean, and it's innocuous
            cancelAlertRule job
            controlAlertRule job

alertOnAnything :: AlertPersistenceState b -> STM (Either (Jobs b) b)
alertOnAnything apState =
    -- TODO should we swap the order of these?
    (Left <$> alertOnNeedRunning (apState ^. updateVar))
        `orElse`
    (Right <$> alertOnStoppedWithException (apState ^. diedQueue))

alertOnNeedRunning :: TMVar (Jobs b) -> STM (Jobs b)
alertOnNeedRunning = takeTMVar

alertOnStoppedWithException :: TBQueue b -> STM b
alertOnStoppedWithException = readTBQueue

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
