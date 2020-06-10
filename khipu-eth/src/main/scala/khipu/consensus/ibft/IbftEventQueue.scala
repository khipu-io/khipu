
package khipu.consensus.ibft

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import khipu.consensus.ibft.ibftevent.IbftEvent

/** Threadsafe queue that lets parts of the system inform the Ibft infrastructure about events */
class IbftEventQueue(messageQueueLimit: Int) {
  private val queue: BlockingQueue[IbftEvent] = new LinkedBlockingQueue[IbftEvent]();

  //private static final Logger LOG = LogManager.getLogger();
  /**
   * Put an Ibft event onto the queue
   *
   * @param event Provided ibft event
   */
  def add(event: IbftEvent) {
    if (queue.size > messageQueueLimit) {
      //LOG.warn("Queue size exceeded trying to add new ibft event {}", event);
    } else {
      queue.add(event);
    }
  }

  def size() = queue.size();

  def isEmpty() = queue.isEmpty();

  /**
   * Blocking request for the next item available on the queue that will timeout after a specified
   * period
   *
   * @param timeout number of time units after which this operation should timeout
   * @param unit the time units in which to count
   * @return The next IbftEvent to become available on the queue or null if the expiry passes
   * @throws InterruptedException If the underlying queue implementation is interrupted
   */
  //@Nullable
  @throws(classOf[InterruptedException])
  def poll(timeout: Long, unit: TimeUnit): IbftEvent = {
    queue.poll(timeout, unit)
  }
}
