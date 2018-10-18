package khipu.crypto.hash

import java.security.MessageDigest;

/**
 * <p>This class is a template which can be used to implement hash
 * functions. It takes care of some of the API, and also provides an
 * internal data buffer whose length is equal to the hash function
 * internal block length.</p>
 *
 * <p>Classes which use this template MUST provide a working {@link
 * #getBlockLength} method even before initialization (alternatively,
 * they may define a custom {@link #getInternalBlockLength} which does
 * not call {@link #getBlockLength}. The {@link #getDigestLength} should
 * also be operational from the beginning, but it is acceptable that it
 * returns 0 while the {@link #doInit} method has not been called
 * yet.</p>
 *
 * <pre>
 * ==========================(LICENSE BEGIN)============================
 *
 * Copyright (c) 2007-2010  Projet RNRT SAPHIR
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * ===========================(LICENSE END)=============================
 * </pre>
 *
 * @version   $Revision: 229 $
 * @author    Thomas Pornin &lt;thomas.pornin@cryptolog.com&gt;
 */

abstract class DigestEngine(alg: String) extends MessageDigest(alg) with Digest {

  /**
   * Reset the hash algorithm state.
   */
  protected def engineReset()

  /**
   * Process one block of data.
   *
   * @param data   the data block
   */
  protected def processBlock(data: Array[Byte])

  /**
   * Perform the final padding and store the result in the
   * provided buffer. This method shall call {@link #flush}
   * and then {@link #update} with the appropriate padding
   * data in order to get the full input data.
   *
   * @param buf   the output buffer
   * @param off   the output offset
   */
  protected def doPadding(buf: Array[Byte], off: Int)

  /**
   * This function is called at object creation time; the
   * implementation should use it to perform initialization tasks.
   * After this method is called, the implementation should be ready
   * to process data or meaningfully honour calls such as
   * {@link #engineGetDigestLength}
   */
  protected def doInit()

  /**
   * Instantiate the engine.
   */
  doInit()
  private var digestLen = engineGetDigestLength()
  private val blockLen = getInternalBlockLength()
  private val inputBuf = Array.ofDim[Byte](blockLen)
  private var outputBuf = Array.ofDim[Byte](digestLen)
  private var inputLen = 0
  private var blockCount: Long = 0L

  private def adjustDigestLen() {
    if (digestLen == 0) {
      digestLen = engineGetDigestLength()
      outputBuf = Array.ofDim[Byte](digestLen)
    }
  }

  final override def digest(): Array[Byte] = {
    adjustDigestLen()
    val result = Array.ofDim[Byte](digestLen)
    digest(result, 0, digestLen)
    result
  }

  final override def digest(input: Array[Byte]): Array[Byte] = {
    update(input, 0, input.length)
    digest()
  }

  final override def digest(buf: Array[Byte], offset: Int, len: Int): Int = {
    adjustDigestLen();
    if (len >= digestLen) {
      doPadding(buf, offset)
      reset()
      digestLen
    } else {
      doPadding(outputBuf, 0)
      System.arraycopy(outputBuf, 0, buf, offset, len)
      reset()
      len
    }
  }

  final override def reset() {
    engineReset()
    inputLen = 0
    blockCount = 0
  }

  final override def update(input: Byte) {
    inputBuf(inputLen) = input.toByte
    inputLen += 1
    if (inputLen == blockLen) {
      processBlock(inputBuf)
      blockCount += 1
      inputLen = 0
    }
  }

  final override def update(input: Array[Byte]) {
    update(input, 0, input.length)
  }

  final override def update(input: Array[Byte], _offset: Int, _len: Int) {
    var offset = _offset
    var len = _len
    while (len > 0) {
      var copyLen = blockLen - inputLen
      if (copyLen > len) {
        copyLen = len
      }
      System.arraycopy(input, offset, inputBuf, inputLen, copyLen)
      offset += copyLen
      inputLen += copyLen
      len -= copyLen
      if (inputLen == blockLen) {
        processBlock(inputBuf)
        blockCount += 1
        inputLen = 0
      }
    }
  }

  /**
   * Get the internal block length. This is the length (in
   * bytes) of the array which will be passed as parameter to
   * {@link #processBlock}. The default implementation of this
   * method calls {@link #getBlockLength} and returns the same
   * value. Overriding this method is useful when the advertised
   * block length (which is used, for instance, by HMAC) is
   * suboptimal with regards to internal buffering needs.
   *
   * @return  the internal block length (in bytes)
   */
  final protected def getInternalBlockLength(): Int = getBlockLength()

  /**
   * Flush internal buffers, so that less than a block of data
   * may at most be upheld.
   *
   * @return  the number of bytes still unprocessed after the flush
   */
  final protected def flush(): Int = inputLen

  /**
   * Get a reference to an internal buffer with the same size
   * than a block. The contents of that buffer are defined only
   * immediately after a call to {@link #flush()}: if
   * {@link #flush()} return the value {@code n}, then the
   * first {@code n} bytes of the array returned by this method
   * are the {@code n} bytes of input data which are still
   * unprocessed. The values of the remaining bytes are
   * undefined and may be altered at will.
   *
   * @return  a block-sized internal buffer
   */
  final protected def getBlockBuffer(): Array[Byte] = inputBuf

  /**
   * Get the "block count": this is the number of times the
   * {@link #processBlock} method has been invoked for the
   * current hash operation. That counter is incremented
   * <em>after</em> the call to {@link #processBlock}.
   *
   * @return  the block count
   */
  final protected def getBlockCount(): Long = blockCount

  /**
   * This function copies the internal buffering state to some
   * other instance of a class extending {@code DigestEngine}.
   * It returns a reference to the copy. This method is intended
   * to be called by the implementation of the {@link #copy}
   * method.
   *
   * @param dest   the copy
   * @return  the value {@code dest}
   */
  final protected def copyState(dest: DigestEngine): Digest = {
    dest.inputLen = inputLen
    dest.blockCount = blockCount
    System.arraycopy(inputBuf, 0, dest.inputBuf, 0, inputBuf.length)
    adjustDigestLen()
    dest.adjustDigestLen()
    System.arraycopy(outputBuf, 0, dest.outputBuf, 0, outputBuf.length)
    dest
  }
}
