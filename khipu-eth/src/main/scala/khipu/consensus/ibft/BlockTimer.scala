package khipu.consensus.ibft

import java.time.Clock
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import khipu.consensus.ibft.ibftevent.BlockTimerExpiry
import khipu.domain.BlockHeader

/** Class for starting and keeping organised block timers */
/**
 * Construct a BlockTimer with primed executor service ready to start timers
 *
 * @param queue The queue in which to put block expiry events
 * @param minimumTimeBetweenBlocksSeconds Minimum timestamp difference between blocks
 * @param ibftExecutors Executor services that timers can be scheduled with
 * @param clock System clock
 */
class BlockTimer(
    queue:                           IbftEventQueue,
    minimumTimeBetweenBlocksSeconds: Long,
    ibftExecutors:                   IbftExecutors,
    clock:                           Clock
) {
  private var currentTimerTask: Option[ScheduledFuture[_]] = None
  private val minimumTimeBetweenBlocksMillis = minimumTimeBetweenBlocksSeconds * 1000

  /** Cancels the current running round timer if there is one */
  def cancelTimer(): Unit = synchronized {
    currentTimerTask foreach { t => t.cancel(false) }
    currentTimerTask = None
  }

  /**
   * Whether there is a timer currently running or not
   *
   * @return boolean of whether a timer is ticking or not
   */
  def isRunning: Boolean = synchronized {
    currentTimerTask.map(t => !t.isDone) getOrElse false
  }

  /**
   * Starts a timer for the supplied round cancelling any previously active block timer
   *
   * @param round The round identifier which this timer is tracking
   * @param chainHeadHeader The header of the chain head
   */
  def startTimer(round: ConsensusRoundIdentifier, chainHeadHeader: BlockHeader): Unit = synchronized {
    cancelTimer();

    val now = clock.millis()

    // absolute time when the timer is supposed to expire
    val expiryTime = chainHeadHeader.unixTimestamp * 1000 + minimumTimeBetweenBlocksMillis

    if (expiryTime > now) {
      val delay = expiryTime - now

      val newTimerRunnable = new Runnable {
        def run() {
          queue.add(new BlockTimerExpiry(round))
        }
      }

      val newTimerTask = ibftExecutors.scheduleTask(newTimerRunnable, delay, TimeUnit.MILLISECONDS);
      currentTimerTask = Option(newTimerTask)
    } else {
      queue.add(new BlockTimerExpiry(round))
    }
  }
}
