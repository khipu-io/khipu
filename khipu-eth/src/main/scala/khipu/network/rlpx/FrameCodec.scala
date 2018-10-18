package khipu.network.rlpx

import akka.util.ByteString
import java.io.IOException
import khipu.rlp
import khipu.rlp.RLPImplicits._
import org.spongycastle.crypto.StreamCipher
import org.spongycastle.crypto.digests.KeccakDigest
import org.spongycastle.crypto.engines.AESEngine
import org.spongycastle.crypto.modes.SICBlockCipher
import org.spongycastle.crypto.params.{ KeyParameter, ParametersWithIV }
import scala.annotation.tailrec

final case class Frame(header: Header, `type`: Int, payload: ByteString)
final case class Header(bodySize: Int, protocol: Int, contextId: Option[Int], totalPacketSize: Option[Int])

final class FrameCodec(private val secrets: Secrets) {

  val HeaderLength = 32
  val MacSize = 16

  private val allZerosIV = Array.ofDim[Byte](16) // auto filled with 0

  private val enc: StreamCipher = {
    val cipher = new SICBlockCipher(new AESEngine)
    cipher.init(true, new ParametersWithIV(new KeyParameter(secrets.aes), allZerosIV))
    cipher
  }

  private val dec: StreamCipher = {
    val cipher = new SICBlockCipher(new AESEngine)
    cipher.init(false, new ParametersWithIV(new KeyParameter(secrets.aes), allZerosIV))
    cipher
  }

  private var unprocessedData: ByteString = ByteString.empty

  private var headerOpt: Option[Header] = None

  /**
   * Note, this method is not reentrant.
   *
   * @param data
   * @return
   */
  def readFrames(data: ByteString): Seq[Frame] = {
    unprocessedData ++= data
    readRecursive()
  }

  @tailrec
  private def readRecursive(framesSoFar: Vector[Frame] = Vector[Frame]()): Seq[Frame] = {
    if (headerOpt.isEmpty) {
      tryReadHeader()
    }

    headerOpt match {
      case Some(header) =>
        val padding = (16 - (header.bodySize % 16)) % 16
        val totalSizeToRead = header.bodySize + padding + MacSize
        if (totalSizeToRead < 0) { // TODO
          //log.debug(s"totalSizeToRead is less than 0: $totalSizeToRead  with bodySize: ${header.bodySize} in FrameCodec.readRecursive(...)")
        }

        if (unprocessedData.length >= totalSizeToRead) {
          val buffer = unprocessedData.take(totalSizeToRead).toArray

          val frameSize = totalSizeToRead - MacSize
          secrets.ingressMac.update(buffer, 0, frameSize)
          dec.processBytes(buffer, 0, frameSize, buffer, 0)

          val tpe = rlp.decode[Int](buffer)

          val pos = rlp.nextElementIndex(buffer, 0)
          val payload = buffer.slice(pos, header.bodySize)
          val macBuffer = Array.ofDim[Byte](secrets.ingressMac.getDigestSize)

          doSum(secrets.ingressMac, macBuffer)
          updateMac(secrets.ingressMac, macBuffer, 0, buffer, frameSize, egress = false)

          headerOpt = None
          unprocessedData = unprocessedData.drop(totalSizeToRead)
          readRecursive(framesSoFar :+ Frame(header, tpe, ByteString(payload)))
        } else {
          framesSoFar
        }

      case None => framesSoFar
    }
  }

  private def tryReadHeader() {
    if (unprocessedData.size >= HeaderLength) {
      val headBuffer = unprocessedData.take(HeaderLength).toArray

      updateMac(secrets.ingressMac, headBuffer, 0, headBuffer, 16, egress = false)

      dec.processBytes(headBuffer, 0, 16, headBuffer, 0)

      val bodySize0 = headBuffer(0).toLong
      val bodySize1 = (bodySize0 << 8) + (headBuffer(1) & 0xFF)
      val bodySize2 = (bodySize1 << 8) + (headBuffer(2) & 0xFF)
      if (bodySize2 < 0 || bodySize2 > Int.MaxValue) { // TODO -- if 
        //log.debug(s"Body size is too large or is negative: $bodySize0 -> $bodySize2 in FrameCodec.tryReadHeader()")
      }

      val rlpList = rlp.decode[Seq[Int]](headBuffer.drop(3))(seqEncDec[Int]).lift
      val protocol = rlpList(0).get
      val contextId = rlpList(1)
      val totalPacketSize = rlpList(2)

      unprocessedData = unprocessedData.drop(HeaderLength)
      headerOpt = Some(Header(bodySize2.toInt, protocol, contextId, totalPacketSize))
    }
  }

  // TODO check and limit size < Int.MaxValue
  def writeFrames(frames: Seq[Frame]): ByteString = {
    val out = frames.zipWithIndex.foldLeft(ByteString.newBuilder) {
      case (acc, (frame, index)) =>
        val firstFrame = index == 0
        val lastFrame = index == frames.size - 1

        val out = ByteString.newBuilder

        val headBuffer = Array.ofDim[Byte](HeaderLength)
        val ptype = rlp.encode(frame.`type`)

        val totalSize =
          if (firstFrame) frame.payload.length + ptype.length
          else frame.payload.length

        headBuffer(0) = (totalSize >> 16).toByte
        headBuffer(1) = (totalSize >> 8).toByte
        headBuffer(2) = totalSize.toByte

        var headerDataElems: Seq[Array[Byte]] = Vector[Array[Byte]]()
        headerDataElems :+= rlp.encode(frame.header.protocol)
        frame.header.contextId.foreach { cid => headerDataElems :+= rlp.encode(cid) }
        frame.header.totalPacketSize foreach { tfs => headerDataElems :+= rlp.encode(tfs) }

        val headerData = rlp.encode(headerDataElems)(seqEncDec[Array[Byte]])
        System.arraycopy(headerData, 0, headBuffer, 3, headerData.length)
        enc.processBytes(headBuffer, 0, 16, headBuffer, 0)
        updateMac(secrets.egressMac, headBuffer, 0, headBuffer, 16, egress = true)

        val buff = Array.ofDim[Byte](256)
        out.putBytes(headBuffer)

        if (firstFrame) {
          // packet-type only in first frame
          enc.processBytes(ptype, 0, ptype.length, buff, 0)
          out.putBytes(buff.take(ptype.length))
          secrets.egressMac.update(buff, 0, ptype.length)
        }

        out ++= processFramePayload(frame.payload)

        if (lastFrame) {
          // padding and mac only in last frame
          out ++= processFramePadding(totalSize)
          out ++= processFrameMac()
        }

        out
    }

    out.result
  }

  private def processFramePayload(payload: ByteString): ByteString = {
    var i = 0
    val out = ByteString.newBuilder

    while (i < payload.length) {
      val bytes = payload.drop(i).take(256).toArray
      enc.processBytes(bytes, 0, bytes.length, bytes, 0)
      secrets.egressMac.update(bytes, 0, bytes.length)
      out.putBytes(bytes)
      i += bytes.length
    }
    out.result
  }

  private def processFramePadding(totalSize: Int): ByteString = {
    val padding = 16 - (totalSize % 16)
    if (padding < 16) {
      val pad = Array.ofDim[Byte](16)
      val buff = Array.ofDim[Byte](16)
      enc.processBytes(pad, 0, padding, buff, 0)
      secrets.egressMac.update(buff, 0, padding)
      ByteString(buff.take(padding))
    } else {
      ByteString()
    }
  }

  private def processFrameMac(): ByteString = {
    val macBuffer = Array.ofDim[Byte](secrets.egressMac.getDigestSize)
    doSum(secrets.egressMac, macBuffer)
    updateMac(secrets.egressMac, macBuffer, 0, macBuffer, 0, egress = true)
    ByteString(macBuffer.take(16))
  }

  private def makeMacCipher(): AESEngine = {
    val macc = new AESEngine()
    macc.init(true, new KeyParameter(secrets.mac))
    macc
  }

  private def updateMac(mac: KeccakDigest, seed: Array[Byte], offset: Int, out: Array[Byte], outOffset: Int, egress: Boolean): Array[Byte] = {
    val aesBlock = Array.ofDim[Byte](mac.getDigestSize)
    doSum(mac, aesBlock)
    makeMacCipher().processBlock(aesBlock, 0, aesBlock, 0)

    val length = 16
    var i = 0
    while (i < length) {
      aesBlock(i) = (aesBlock(i) ^ seed(i + offset)).toByte
      i += 1
    }

    mac.update(aesBlock, 0, length)
    val result = Array.ofDim[Byte](mac.getDigestSize)
    doSum(mac, result)

    if (egress) {
      System.arraycopy(result, 0, out, outOffset, length)
    } else {
      var i = 0
      while (i < length) {
        if (out(i + outOffset) != result(i)) {
          throw new IOException("MAC mismatch")
        }
        i += 1
      }
    }

    result
  }

  private def doSum(mac: KeccakDigest, out: Array[Byte]) = {
    new KeccakDigest(mac).doFinal(out, 0)
  }

}
