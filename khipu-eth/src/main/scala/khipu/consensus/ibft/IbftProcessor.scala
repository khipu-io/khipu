package khipu.consensus.ibft

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit; import khipu.consensus.ibft.ibftevent.IbftEvent

/**
 * Execution context for draining queued ibft events and applying them to a maintained state
 * @param incomingQueue The event queue from which to drain new events
 * @param eventMultiplexer an object capable of handling any/all IBFT events
 */
class IbftProcessor(incomingQueue: IbftEventQueue, eventMultiplexer: EventMultiplexer) extends Runnable {

  @volatile private var shutdown = false;
  private val shutdownLatch = new CountDownLatch(1)

  /** Indicate to the processor that it should gracefully stop at its next opportunity */
  def stop() {
    shutdown = true;
  }

  @throws(classOf[InterruptedException])
  def awaitStop() {
    shutdownLatch.await();
  }

  override def run() {
    try {
      while (!shutdown) {
        nextIbftEvent().ifPresent(eventMultiplexer :: handleIbftEvent);
      }
    } catch {
      case t: Throwable =>
      //LOG.error("IBFT Mining thread has suffered a fatal error, mining has been halted", t);
    }
    // Clean up the executor service the round timer has been utilising
    //LOG.info("Shutting down IBFT event processor");
    shutdownLatch.countDown();
  }

  private def nextIbftEvent(): Option[IbftEvent] = {
    try {
      Option(incomingQueue.poll(500, TimeUnit.MILLISECONDS));
    } catch {
      case interrupt: InterruptedException =>
        // If the queue was interrupted propagate it and spin to check our shutdown status
        Thread.currentThread().interrupt()
        return None
    }
  }
}
