package khipu.network.rlpx

import akka.util.ByteString
import java.util.concurrent.atomic.AtomicInteger
import khipu.network.p2p.{ Message, MessageDecoder, MessageSerializable }
import scala.util.Failure
import scala.util.Success

final class MessageCodec(frameCodec: FrameCodec, messageDecoder: MessageDecoder, protocolVersion: Message.Version) {

  val MaxFramePayloadSize: Int = Int.MaxValue // no framing

  val contextIdCounter = new AtomicInteger

  def decodeMessages(data: ByteString): (Seq[Message], Seq[Throwable]) = {
    try {
      val frames = frameCodec.readFrames(data)
      (frames.foldLeft((Vector[Message](), Vector[Throwable]())) {
        case ((ss, es), Frame(_, tpe, payload)) =>
          messageDecoder.fromBytes(tpe, payload.toArray, protocolVersion) match {
            case Success(m) => (ss :+ m, es)
            case Failure(e) => (ss, es :+ e)
          }
      })
    } catch {
      case e: Throwable => (Vector(), Vector(e))
    }
  }

  def encodeMessage(serializable: MessageSerializable): ByteString = {
    val encoded: Array[Byte] = serializable.toBytes
    val numFrames = Math.ceil(encoded.length / MaxFramePayloadSize.toDouble).toInt
    val contextId = contextIdCounter.incrementAndGet()

    val frames = (0 until numFrames) map { frameNo =>
      val payload = encoded.drop(frameNo * MaxFramePayloadSize).take(MaxFramePayloadSize)
      val totalPacketSize = if (frameNo == 0) Some(encoded.length) else None
      val header =
        if (numFrames > 1) Header(payload.length, 0, Some(contextId), totalPacketSize)
        else Header(payload.length, 0, None, None)
      Frame(header, serializable.code, ByteString(payload))
    }

    frameCodec.writeFrames(frames)
  }

}
