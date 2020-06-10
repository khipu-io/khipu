package khipu.consensus.ibft

import static org.hyperledger.besu.ethereum.eth.manager.MonitoredExecutors.newScheduledThreadPool;

import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

object IbftExecutors {
  
  private trait State
  private object State {
    case object IDLE extends State
    case object RUNNING extends State
    case object STOPPED extends State
  }

  def create(metricsSystem: MetricsSystem ) IbftExecutors =  {
    new IbftExecutors(metricsSystem);
  }
}
class IbftExecutors private (metricsSystem: MetricsSystem) {
  import IbftExecutors.State

//  private static final Logger LOG = LogManager.getLogger();

  private final Duration shutdownTimeout = Duration.ofSeconds(30);

  @volatile private var timerExecutor: ScheduledExecutorService = _ 
  @volatile private var ibftProcessorExecutor: ExecutorService = _ ;
  @volatile private var state: State = State.IDLE



  def start(): Unit = synchronized  {
    if (state != State.IDLE) {
      // Nothing to do
      return;
    }
    state = State.RUNNING;
    ibftProcessorExecutor = Executors.newSingleThreadExecutor();
    timerExecutor = newScheduledThreadPool("IbftTimerExecutor", 1, metricsSystem);
  }

  def stop(): Unit = {
    this.synchronized {
      if (state != State.RUNNING) {
        return;
      }
      state = State.STOPPED
    }

    timerExecutor.shutdownNow();
    ibftProcessorExecutor.shutdownNow();
  }

  @throws(classOf[InterruptedException])
  def awaitStop()  {
    if (!timerExecutor.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS)) {
      //LOG.error("{} timer executor did not shutdown cleanly.", getClass().getSimpleName());
    }
    if (!ibftProcessorExecutor.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS)) {
      //LOG.error("{} ibftProcessor executor did not shutdown cleanly.", getClass().getSimpleName());
    }
  }

  def  executeIbftProcessor(ibftProcessor:IbftProcessor ): Unit = synchronized {
    assertRunning();
    ibftProcessorExecutor.execute(ibftProcessor);
  }

  def  scheduleTask(command:Runnable , delay: Long, unit: TimeUnit ) : ScheduledFuture[_] = synchronized {
    assertRunning();
    timerExecutor.schedule(command, delay, unit)
  }

  private def assertRunning() {
    if (state != State.RUNNING) {
      throw new IllegalStateException(
          "Attempt to interact with "
              + getClass().getSimpleName()
              + " that is not running. Current State is "
              + state
              + ".");
    }
  }
}
