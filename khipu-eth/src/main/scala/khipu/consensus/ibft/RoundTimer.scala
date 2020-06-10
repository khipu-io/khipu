package khipu.consensus.ibft

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit
import khipu.consensus.ibft.ibftevent.RoundExpiry

/**
 * Class for starting and keeping organised round timers
 *
 * @param queue The queue in which to put round expiry events
 * @param baseExpirySeconds The initial round length for round 0
 * @param ibftExecutors executor service that timers can be scheduled with
 */
class RoundTimer(queue: IbftEventQueue, baseExpirySeconds: Long, ibftExecutors: IbftExecutors) {
  private var currentTimerTask: Option[ScheduledFuture[_]] = None
  private val baseExpiryMillis = baseExpirySeconds * 1000

  /** Cancels the current running round timer if there is one */
  def cancelTimer(): Unit = synchronized {
    currentTimerTask.foreach(t => t.cancel(false))
    currentTimerTask = None
  }

  /**
   * Whether there is a timer currently running or not
   *
   * @return boolean of whether a timer is ticking or not
   */
  def isRunning(): Boolean = synchronized {
    currentTimerTask.map(t => !t.isDone()).getOrElse(false)
  }

  /**
   * Starts a timer for the supplied round cancelling any previously active round timer
   *
   * @param round The round identifier which this timer is tracking
   */
  def startTimer(round: ConsensusRoundIdentifier): Unit = synchronized {
    cancelTimer();

    val expiryTime = baseExpiryMillis * math.pow(2, round.round).toLong

    val newTimerRunnable = new Runnable() {
      def run() {
        queue.add(new RoundExpiry(round))
      }
    }

    val newTimerTask = ibftExecutors.scheduleTask(newTimerRunnable, expiryTime, TimeUnit.MILLISECONDS)
    currentTimerTask = Option(newTimerTask)
  }
}
