/*
 Copyright (c) 2015-2016 Simon Klein, Google Inc.
 All rights reserved.
 
 Redistribution and use in source and binary forms, with or without 
 modification, are permitted provided that the following conditions are met:
 1. Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright notice, 
    this list of conditions and the following disclaimer in the documentation 
    and/or other materials provided with the distribution.
 
 THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND ANY 
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
 DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY 
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON 
 ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
 SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 
 The views and conclusions contained in the software and documentation are those 
 of the authors and should not be interpreted as representing official policies, 
 either expressed or implied, of the Huldra Project.
*/

package khipu

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import scala.util.control.Breaks._

/**
 * Copyright (c) 2015-2016 The Huldra Project.
 * See the above LICENSE for too long unnecessary boring license bullshit that otherwise would be written here.
 * Tl;dr: Use this possibly broken code however you like.
 *
 * Representation:
 * Base is 2^32.
 * Magnitude as array in little endian order.
 * The len first ints count as the number.
 * Sign is represented by a sign int (-1 or 1).
 * Internally zero is allowed to have either sign. (Otherwise one would have to remember to check for sign-swap for div/mul etc...)
 * Zero has length 1 and dig[0]==0.
 *
 * Principle: No Exceptions.
 * If a programmer divides by zero he has only himself to blame. It is OK to have undefined behavior.
 *
 * Beware:
 * Nothing should be assumed about a position of the internal array that is not part of the number, e.g. that it is 0.
 * Beware of signed extensions!
 *
 * Organization: Locality of reference
 * Stuff that has something in common should generally be close to oneanother in the code.
 * So stuff regarding multiplication is bunched together.
 *
 * Coding style: Klein / As long as it looks good
 * Generally brackets on new line, but exception can be made for small special cases, then they may be aligned on the same line.
 * Never space after for or if or akin, it looks ugly.
 * Bracketless loops may be on one line. For nested bracketless loops each should be indented on a new line.
 */
object BigInt {

  // --- simple test
  def main(args: Array[String]) {
    import TEST._
    constructorTest()
    addTest()
    subTest()
    mulTest()
    divTest()
    remTest()
    longDivTest()
    testLongCastInMul()
    testLongZeroAdd()
    testDivAndRem()
    testBitShifts()
    testSetClearFlipTestBit()
    testAnd()
    testOr()
    testXor()
    testAndNot()
    testNot()
    testLongAdd()
    testMod()
  }

  private object TEST {
    import java.math.BigInteger

    private val rnd = new java.util.Random()
    private def getRndNumber(len: Int): Array[Char] = {
      val sign = rnd.nextInt(2)
      val num = Array.ofDim[Char](len + sign)
      if (sign > 0) num(0) = '-'
      num(sign) = ('1' + rnd.nextInt(9)).toChar
      var i = sign + 1
      while (i < len + sign) {
        num(i) = ('0' + rnd.nextInt(10)).toChar
        i += 1
      }
      return num
    }

    private def assertEquals(msg: String, a: Any, b: Any) = assert(a == b, s"$msg: $a != $b")

    def constructorTest() {
      val s = "246313781983713469235139859013498018470170100003957203570275438387"
      val ans = new BigInt(s).toString
      assertEquals("Error in toString()", s, ans)
      assertEquals("Error 4M", "4000000000", new BigInt("4000000000").toString)
      assertEquals("Error", "3928649759", new BigInt("3928649759").toString)
      var me = new BigInt(s)
      me.umul(0)
      assertEquals("Zero string", "0", me.toString)
      me = new BigInt("0")
      assertEquals("Zero string2", "0", me.toString)
      val littleEndian = Array[Byte](35, 47, 32, 45, 93, 0, 1, 0, 0, 0, 0, 0)
      val bigEndian = Array[Byte](1, 0, 93, 45, 32, 47, 35)
      assertEquals("Byte[] constructor", new BigInteger(1, bigEndian).toString(), new BigInt(1, littleEndian, 10).toString)
      assertEquals("Byte[] 0 constructor", "0", new BigInt(1, Array[Byte](0, 0, 0), 3).toString)
      //Add test case covering length-increase due to add in mulAdd().
    }

    def addTest() {
      val s = "246313781983713469235139859013498018470170100003957203570275438387"
      val t = "2374283475698324756873245832748"
      var facit = new BigInteger(s).add(new BigInteger(t))

      var me = new BigInt(s)
      me.add(new BigInt(t))
      assertEquals("Add", facit.toString, me.toString)

      me = new BigInt(t)
      me.add(new BigInt(s))
      assertEquals("Add2", facit.toString, me.toString)

      facit = new BigInteger(s)
      facit = facit.add(facit)
      me.assign(s)
      me.add(me)
      assertEquals("Add3", facit.toString, me.toString)

      facit = new BigInteger(t)
      facit = facit.add(facit)
      me.assign(t)
      me.add(me)
      assertEquals("Add4", facit.toString, me.toString)

      me = new BigInt("0")
      facit = BigInteger.ZERO
      var i = 0
      while (i < 1337) {
        var tmp = rnd.nextLong() & ((1L << 32) - 1)
        me.uadd(tmp.toInt)
        facit = facit.add(BigInteger.valueOf(tmp))
        assertEquals("For-loop " + i + ": " + me + " " + facit + "\nAdded: " + tmp, facit.toString, me.toString)
        tmp = rnd.nextLong() >>> 1
        me.uadd(tmp)
        facit = facit.add(BigInteger.valueOf(tmp))
        assertEquals("For-loop2 " + i + ": " + me + " " + facit + "\nAdded: " + tmp, facit.toString, me.toString)
        i += 1
      }
    }

    def subTest() {
      val s = "246313781983713469235139859013498018470170100003957203570275438387"
      val t = "2374283475698324756873245832748"
      var me = new BigInt(s)
      me.sub(new BigInt(s))
      assertEquals("Sub to zero", "0", me.toString)
      me = new BigInt(t)
      me.sub(new BigInt(t))
      assertEquals("Sub2 to zero", "0", me.toString)
      me = new BigInt("1337")
      me.usub(1337)
      assertEquals("Small sub", "0", me.toString)
      me = new BigInt("4000000000")
      me.sub(new BigInt("2000000000"))
      assertEquals("Small sub", "2000000000", me.toString)

      var facit = new BigInteger(s).subtract(new BigInteger(t))
      me = new BigInt(s)
      me.sub(new BigInt(t))
      assertEquals("Sub", facit.toString, me.toString)

      facit = new BigInteger(t).subtract(new BigInteger(s))
      me = new BigInt(t)
      var tmp = new BigInt("-" + s)
      me.add(tmp)
      assertEquals("Sub2", facit.toString, me.toString)

      me.umul(0)
      me.usub(1)
      assertEquals("From 0 to -1", "-1", me.toString)
      me.mul(-16)
      assertEquals("From -1 to 16", "16", me.toString)
      me.div(-4)
      assertEquals("From 16 to -4", "-4", me.toString)
    }

    def mulTest() {
      var me = new BigInt("2000000000")
      me.umul(3)
      assertEquals("Small", "6000000000", me.toString)
      me = new BigInt("4000000000")
      me.mul(me)
      assertEquals("Two", "16000000000000000000", me.toString)

      val s = "246313781983713469235139859013498018470170100003957203570275438387"
      val t = "2374283475698324756873245832748"
      var facit = new BigInteger(s).multiply(new BigInteger(t))

      me = new BigInt(t)
      me.mul(new BigInt(s))
      assertEquals("Mul ", facit.toString, me.toString)

      me.umul(0)
      me.uadd(1)
      assertEquals("0 to 1", "1", me.toString)
      me.mul(new BigInt(s))
      assertEquals("1 to s", s, me.toString)
      me.mul(new BigInt(t))
      assertEquals("Mul2", facit.toString(), me.toString)

      facit = new BigInteger(1, Array[Byte](-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
      me = new BigInt(1, Array[Int](-1, -1, -1, -1), 4)
      var ulong = new BigInteger(1, Array[Byte](-1, -1, -1, -1, -1, -1, -1, -1))
      var i = 0
      while (i < 256) {
        facit = facit.multiply(ulong)
        me.umul(-1L)
        assertEquals("Long mul " + i, facit.toString, me.toString)
        i += 1
      }
    }

    def divTest() {
      var s = "246313781983713469235139859013498018470170100003957203570275438387"
      var t = "2374283475698324756873245832748"
      var facit = new BigInteger(s).divide(BigInteger.valueOf(1337))
      var me = new BigInt(s)
      me.udiv(1337)
      assertEquals("Div ", facit.toString(), me.toString)

      facit = new BigInteger(s + t + s).divide(BigInteger.valueOf((1L << 32) - 1))
      me = new BigInt(s + t + s)
      me.udiv(-1)
      assertEquals("Div2 ", facit.toString(), me.toString)

      facit = new BigInteger(s).divide(new BigInteger(t))
      me = new BigInt(s)
      var tmp = new BigInt(t)
      me.div(tmp)
      assertEquals("Div3 ", facit.toString(), me.toString)

      me.div(new BigInt(s))
      assertEquals("Should be 0", "0", me.toString)

      facit = new BigInteger(s).divide(new BigInteger("-" + t + t))
      me.assign(s)
      tmp.assign("-" + t + t)
      me.div(tmp)
      assertEquals("Div4 ", facit.toString(), me.toString)

      s = "253187224242823454860064468797249161593623134834254603067018"
      t = "434771785074759645588146668555"
      facit = new BigInteger(s).divide(new BigInteger(t))
      me = new BigInt(s)
      me.div(new BigInt(t))
      assertEquals("Div5 ", facit.toString(), me.toString)

      val m = Array[Int](2, 2, 2, 2)
      val n = Array[Int](1, 2, 2, 2)
      val u = Array[Array[Int]](Array[Int](0xfffe0000, 0x8000), Array[Int](0x00000003, 0x8000), Array[Int](0, 0x7fff8000), Array[Int](0xfffe0000, 0x80000000))
      val v = Array[Array[Int]](Array[Int](0x8000ffff), Array[Int](0x00000001, 0x2000), Array[Int](1, 0x8000), Array[Int](0xffff, 0x8000))
      val q = Array[Array[Int]](Array[Int](0xffff), Array[Int](0x0003), Array[Int](0xfffe), Array[Int](0xffff))
      val r = Array[Array[Int]](Array[Int](0x7fffffff), Array[Int](0, 0x2000), Array[Int](0xffff0002, 0x7fff), Array[Int](0xffffffff, 0x7fff))
      var i = 0
      while (i < 4) {
        val a = new BigInt(1, u(i), m(i))
        val b = new BigInt(1, v(i), n(i))
        val rem = a.divRem(b)
        assertEquals("Hack div " + i, new BigInt(1, q(i), q(i).length).toString, a.toString)
        assertEquals("Hack rem " + i, new BigInt(1, r(i), r(i).length).toString, rem.toString)
        i += 1
      }

      s = "170141183460469231750134047781003722752"
      t = "39614081257132168801066942463"
      me = new BigInt(s)
      //me = new BigInt(1, new int[]{0,0xfffffffe,0,0x80000000}, 4)
      tmp = new BigInt(t)
      //tmp = new BigInt(1, new int[]{0xffffffff,0,0x80000000}, 3)
      var rr = me.divRem(tmp)
      facit = new BigInteger(s).divide(new BigInteger(t))
      assertEquals("Div shift-32 ", facit.toString(), me.toString)
      facit = new BigInteger(s).remainder(new BigInteger(t))
      assertEquals("Rem shift-32 ", facit.toString(), rr.toString)

      me = new BigInt(1, Array[Int](0, 0, 0x80000000, 0x7fffffff), 4)
      tmp = new BigInt(1, Array[Int](1, 0, 0x80000000), 3)
      var ans = new BigInteger(me.toString).divideAndRemainder(new BigInteger(tmp.toString))
      rr = me.divRem(tmp)
      assertEquals("Div addback ", ans(0).toString, me.toString)
      assertEquals("Rem addback ", ans(1).toString, rr.toString)

      me = new BigInt(1, Array[Int](0x0003, 0x0000, 0x80000000), 3)
      tmp = new BigInt(1, Array[Int](0x0001, 0x0000, 0x20000000), 3)
      ans = new BigInteger(me.toString).divideAndRemainder(new BigInteger(tmp.toString))
      rr = me.divRem(tmp)
      assertEquals("Div addback2 ", ans(0).toString, me.toString)
      assertEquals("Rem addback2 ", ans(1).toString, rr.toString)

      me = new BigInt(1, Array[Int](0x0000, 0xfffffffe, 0x80000000), 3)
      tmp = new BigInt(1, Array[Int](0xffffffff, 0x80000000), 2)
      ans = new BigInteger(me.toString).divideAndRemainder(new BigInteger(tmp.toString))
      rr = me.divRem(tmp)
      assertEquals("Div qhat=b+1 ", ans(0).toString(), me.toString)
      assertEquals("Rem qhat=b+1 ", ans(1).toString(), rr.toString)
    }

    def remTest() {
      var s = "246313781983713469235139859013498018470170100003957203570275438387"
      var t = "2374283475698324756873245832748"
      var facit = new BigInteger(s).remainder(BigInteger.valueOf(1337))
      var me = new BigInt(s)
      me.urem(1337)
      assertEquals("Rem ", facit.toString(), me.toString)

      facit = new BigInteger(s + t + s).remainder(BigInteger.valueOf((1L << 32) - 1))
      me = new BigInt(s + t + s)
      me.urem(-1)
      assertEquals("Rem2 ", facit.toString(), me.toString)

      facit = new BigInteger(s).remainder(new BigInteger(t))
      me = new BigInt(s)
      var tmp = new BigInt(t)
      me.rem(tmp)
      assertEquals("Rem3 ", facit.toString(), me.toString)

      me.rem(me)
      assertEquals("Should be 0", "0", me.toString)

      facit = new BigInteger(s).remainder(new BigInteger("-" + t + t))
      me.assign(s)
      tmp.assign("-" + t + t)
      me.rem(tmp)
      assertEquals("Rem4 ", facit.toString(), me.toString)
    }

    def longDivTest() { //Division test using long as parameter.
      var i = 0
      while (i < 100) { //System.err.println(i+" divs")
        val s = getRndNumber(1 + i * 10)
        var facit = new BigInteger(new String(s))
        val dividend = new BigInt(s)
        while (!dividend.isZero()) {
          val d = rnd.nextLong()
          if (d != 0) {
            val div = Array.ofDim[Byte](8)
            var tmp = d
            var j = 7
            while (j >= 0) {
              div(j) = (tmp & 0xFF).toByte
              j -= 1
              tmp >>>= 8
            }
            val divb = new BigInteger(1, div)
            val facitBefore = facit
            facit = facit.divide(divb)
            dividend.udiv(d)
            assertEquals(s"$facitBefore / $d($divb)", facit.toString, dividend.toString)
          }
        }
        i += 1
      }
    }

    def testLongCastInMul() {
      val a = new BigInt("1000000000000000")
      val b = new BigInt("1000000000000000")
      a.mul(b)
      assertEquals("10^15 * 10^15", "1000000000000000000000000000000", a.toString)
    }

    def testLongZeroAdd() {
      var a = new BigInt(0)
      a.add(0L)
      assertEquals("add(0L)", true, a.isZero())
      a.uadd(0L)
      assertEquals("uadd(0L)", true, a.isZero())
      a.add(-1L)
      a.add(2L)
      a.add(-1L)
      assertEquals("-1L + 2L + -1L = 0", true, a.isZero())
      a.usub(7L)
      a.sub(-8L)
      assertEquals("-7L - -8L != 0", false, a.isZero())
      a.sub(1L)
      assertEquals("1 - 1L = 0", true, a.isZero())
    }

    def testDivAndRem() {
      // Check divRem
      {
        val p = new BigInt(104608886616216589L)
        val q = new BigInt(104608886616125069L)
        assertEquals("divRem", "91520", p.divRem(q).toString)
        assertEquals("divRem", "1", p.toString)
        assertEquals("divRem", "104608886616125069", q.toString)
      }
      // Check div
      {
        val p = new BigInt(104608886616216589L)
        val q = new BigInt(104608886616125069L)
        p.div(q)
        assertEquals("div", "1", p.toString)
        assertEquals("div", "104608886616125069", q.toString)
      }
      // Check rem
      {
        val p = new BigInt(104608886616216589L)
        val q = new BigInt(104608886616125069L)
        p.rem(q)
        assertEquals("rem", "91520", p.toString)
        assertEquals("rem", "104608886616125069", q.toString)
      }
      // Check udiv
      {
        val p = new BigInt(104608886616216589L)
        val r = p.udiv(104608886616125069L)
        assertEquals("udiv", 91520L, r)
        assertEquals("udiv", "1", p.toString)
      }
      // Check when cumulative remainder overflows signed long.
      {
        val p = "3518084509561074142646556904312376320315226377906768127516"
        val q = "4101587990"
        val a = new BigInteger(p)
        var b = new BigInteger(q)
        val aa = new BigInt(p)
        var bb = new BigInt(q)
        val ans = a.divideAndRemainder(b)
        bb = aa.divRem(bb)
        assertEquals("udiv handles negative long", ans(0).toString, aa.toString)
        assertEquals("udiv handles negative long", ans(1).toString, bb.toString)
      }
    }

    def testBitShifts() {
      val a = new BigInt("45982486592486793248673294867398579368598675986739851099")
      val b = new BigInt("45982486592486793248673294867398579368598675986739851099")
      a.shiftLeft(3673)
      a.shiftRight(3673)
      assertEquals("Left+Right shift", b.toString, a.toString)
    }

    def testSetClearFlipTestBit() {
      var a = new BigInt(1)
      a.shiftLeft(1337)
      var b = new BigInt(0)
      b.setBit(1337)
      assertEquals("Set bit", a.toString, b.toString)
      assertEquals("Test bit", true, a.testBit(1337))
      assertEquals("Test bit", false, a.testBit(1336))
      b.clearBit(1337)
      assertEquals("Clear bit", true, b.isZero())
      assertEquals("Test bit", false, b.testBit(1337))
      b.flipBit(1337)
      assertEquals("Flip bit", a.toString, b.toString)
      b.flipBit(1337)
      assertEquals("Flip bit", true, b.isZero())
      b = new BigInt("24973592847598349867938576938752986459872649249832748")
      var facit = new BigInteger("24973592847598349867938576938752986459872649249832748")
      b.flipBit(77)
      facit = facit.flipBit(77)
      assertEquals("Flip bit", facit.toString, b.toString)
      b.flipBit(0)
      facit = facit.flipBit(0)
      assertEquals("Flip bit", facit.toString, b.toString)
      b.flipBit(31)
      facit = facit.flipBit(31)
      assertEquals("Flip bit", facit.toString, b.toString)
      b.flipBit(32)
      facit = facit.flipBit(32)
      assertEquals("Flip bit", facit.toString, b.toString)
      var i = 0
      while (i < 2048) {
        val s = getRndNumber(1 + rnd.nextInt(100))
        var bit = rnd.nextInt(600)
        a.assign(s)
        facit = new BigInteger(new String(s))
        assertEquals("Random test", facit.testBit(bit), a.testBit(bit))
        bit = rnd.nextInt(600)
        facit = facit.setBit(bit)
        a.setBit(bit)
        assertEquals("Random set", facit.toString, a.toString)
        bit = rnd.nextInt(600)
        facit = facit.clearBit(bit)
        a.clearBit(bit)
        assertEquals("Random clear", facit.toString, a.toString)
        bit = rnd.nextInt(600)
        facit = facit.flipBit(bit)
        a.flipBit(bit)
        assertEquals("Random flip", facit.toString, a.toString)
        i += 1
      }
      a.assign(-1)
      a.shiftLeft(31 + 512)
      a.flipBit(31 + 512)
      b.assign(-1)
      b.shiftLeft(32 + 512)
      assertEquals("Edge flip", b.toString, a.toString)
    }

    def testAnd() {
      var a = new BigInt(1L << 47)
      a.and(new BigInt(0L))
      assertEquals("And with 0", true, a.isZero())
      var i = 0
      while (i < 1024) {
        val s = getRndNumber(1 + rnd.nextInt(64))
        val t = getRndNumber(1 + rnd.nextInt(64))
        val sh1 = rnd.nextInt(4) * 32
        val sh2 = rnd.nextInt(4) * 32
        a.assign(s)
        a.shiftLeft(sh1)
        var facit = new BigInteger(new String(s)).shiftLeft(sh1)
        var b = new BigInt(t)
        b.shiftLeft(sh2)
        a.and(b)
        facit = facit.and(new BigInteger(new String(t)).shiftLeft(sh2))
        assertEquals(s"$i Random and", facit.toString(), a.toString)
        i += 1
      }
      a.assign(-11)
      var b = new BigInt(-6)
      a.and(b)
      assertEquals("-11 & -6 == ", "-16", a.toString)
      a.assign(-11)
      a.shiftLeft(28)
      b.shiftLeft(28)
      a.and(b)
      b.assign(-1)
      b.shiftLeft(32)
      assertEquals("-11<<28 & -6<<28 == -1<<32", b.toString, a.toString)
    }

    def testOr() {
      var a = new BigInt(0)
      a.or(new BigInt(1L << 47))
      var b = new BigInt(1)
      b.shiftLeft(47)
      assertEquals("Or with 0", b.toString, a.toString)
      a.or(new BigInt(-1))
      assertEquals("Or with -1", "-1", a.toString)
      a.assign(-2)
      a.or(new BigInt(1))
      assertEquals("-2 or 1 = ", "-1", a.toString)
      a.assign(1L)
      a.or(new BigInt(-2))
      assertEquals("1 or -2 = ", "-1", a.toString)
      a.or(new BigInt(0))
      assertEquals("-1 or 0 = ", "-1", a.toString)
      var i = 0
      while (i < 1024) {
        var s = getRndNumber(1 + rnd.nextInt(64))
        var t = getRndNumber(1 + rnd.nextInt(64))
        val sh1 = rnd.nextInt(4) * 32
        val sh2 = rnd.nextInt(4) * 32
        a.assign(s)
        b.assign(t)
        a.shiftLeft(sh1)
        b.shiftLeft(sh2)
        var facit = new BigInteger(new String(s)).shiftLeft(sh1)
        a.or(b)
        facit = facit.or(new BigInteger(new String(t)).shiftLeft(sh2))
        assertEquals("Random or", facit.toString, a.toString)
        i += 1
      }
      a = new BigInt(-1)
      a.shiftLeft(2048)
      b.assign(1)
      b.shiftLeft(2048)
      b.sub(1)
      a.or(b)
      assertEquals("-2^2048 or 2^2048-1 = ", "-1", a.toString)
    }

    def testXor() {
      var a = new BigInt(0)
      a.xor(new BigInt(1L << 47))
      var b = new BigInt(1)
      b.shiftLeft(47)
      assertEquals("Xor with 0", b.toString, a.toString)
      a.xor(b)
      assertEquals("Double xor is zero", true, a.isZero())
      var i = 0
      while (i < 1024) {
        var s = getRndNumber(1 + rnd.nextInt(64))
        var t = getRndNumber(1 + rnd.nextInt(64))
        val sh1 = rnd.nextInt(4) * 32
        val sh2 = rnd.nextInt(4) * 32
        a.assign(s)
        b.assign(t)
        a.shiftLeft(sh1)
        b.shiftLeft(sh2)
        var facit = new BigInteger(new String(s)).shiftLeft(sh1)
        a.xor(b)
        facit = facit.xor(new BigInteger(new String(t)).shiftLeft(sh2))
        assertEquals("Random xor", facit.toString, a.toString)
        i += 1
      }
    }

    def testAndNot() {
      var a = new BigInt(1L << 47)
      a.andNot(new BigInt(0))
      var b = new BigInt(1)
      b.shiftLeft(47)
      assertEquals("AndNot with 0", b.toString, a.toString)
      a.andNot(b)
      assertEquals("Self andNot is zero", true, a.isZero())
      var i = 0
      while (i < 1024) {
        val s = getRndNumber(1 + rnd.nextInt(64))
        val t = getRndNumber(1 + rnd.nextInt(64))
        val sh1 = rnd.nextInt(4) * 32
        val sh2 = rnd.nextInt(4) * 32
        a.assign(s)
        a.shiftLeft(sh1)
        var facit = new BigInteger(new String(s)).shiftLeft(sh1)
        b.assign(t)
        b.shiftLeft(sh2)
        a.andNot(b)
        facit = facit.andNot(new BigInteger(new String(t)).shiftLeft(sh2))
        assertEquals("Random andNot", facit.toString, a.toString)
        i += 1
      }
      a.assign(-11)
      b = new BigInt(5)
      a.andNot(b)
      assertEquals("-11 & ~5 == ", "-16", a.toString)
      a.assign(-11)
      a.shiftLeft(28)
      b.assign(~(-6 << 28))
      a.andNot(b)
      b.assign(-1)
      b.shiftLeft(32)
      assertEquals("-11<<28 & ~~(-6<<28) == -1<<32", b.toString, a.toString)
    }

    def testNot() {
      var a = new BigInt(0L)
      a.not()
      assertEquals("~0 = ", "-1", a.toString())
      a.not()
      assertEquals("~~0", true, a.isZero())
      var i = 0
      while (i < 1024) {
        val s = getRndNumber(1 + rnd.nextInt(64))
        val sh1 = rnd.nextInt(4) * 32
        a.assign(s)
        a.shiftLeft(sh1)
        a.not()
        var facit = new BigInteger(new String(s)).shiftLeft(sh1).not()
        assertEquals("Random not", facit.toString, a.toString)
        i += 1
      }
    }

    def testLongAdd() {
      var a = new BigInt(0)
      a.add(-1L)
      assertEquals("Long add", "-1", a.toString)
      a.assign(1L << 40)
      a.assign(0)
      a.add(-1L)
      assertEquals("Long add", "-1", a.toString)
    }

    def testMod() {
      var i = 0
      while (i < 1024) {
        var s = getRndNumber(1 + rnd.nextInt(64))
        var t = getRndNumber(1 + rnd.nextInt(64))
        var a = new BigInt(s)
        var b = new BigInt(t)
        var aa = new BigInteger(new String(s))
        var bb = new BigInteger(new String(t))
        if (bb.compareTo(BigInteger.ZERO) <= 0) {
          bb = bb.negate().add(BigInteger.ONE)
          b.mul(-1)
          b.add(1)
        }
        a.mod(b)
        assertEquals("Random mod", aa.mod(bb).toString(), a.toString())
        i += 1
      }
    }
  }

  /**
   * Used to cast a (base 2^32) digit to a long without getting unwanted sign extension.
   */
  private val MASK = (1L << 32) - 1

  /*** <Mul Helper> ***/
  /**
   * Multiplies two magnitude arrays and returns the result.
   *
   * @param u		The first magnitude array.
   * @param ulen	The length of the first array.
   * @param v		The second magnitude array.
   * @param vlen	The length of the second array.
   * @return		A ulen+vlen length array containing the result.
   * @complexity	O(n^2)
   */
  private def naiveMul(u: Array[Int], ulen: Int, v: Array[Int], vlen: Int): Array[Int] = {
    val res = Array.ofDim[Int](ulen + vlen)
    var carry = 0L
    var tmp = 0L
    var ui = u(0) & MASK
    var j = 0
    while (j < vlen) {
      tmp = ui * (v(j) & MASK) + carry
      res(j) = tmp.toInt
      carry = tmp >>> 32
      j += 1
    }
    res(vlen) = carry.toInt
    var i = 1
    while (i < ulen) {
      ui = u(i) & MASK
      carry = 0
      var j = 0
      while (j < vlen) {
        tmp = ui * (v(j) & MASK) + (res(i + j) & MASK) + carry
        res(i + j) = tmp.toInt
        carry = tmp >>> 32
        j += 1
      }
      res(i + vlen) = carry.toInt
      i += 1
    }
    return res
  }

  /**
   * Multiplies partial magnitude arrays x[off..off+n) and y[off...off+n) and returns the result.
   * Algorithm: Karatsuba
   *
   * @param x		The first magnitude array.
   * @param y		The second magnitude array.
   * @param off	The offset, where the first element is residing.
   * @param n		The length of each of the two partial arrays.
   * @complexity	O(n^1.585)
   */
  private def kmul(x: Array[Int], y: Array[Int], off: Int, n: Int): Array[Int] = {
    // x = x1*B^m + x0
    // y = y1*B^m + y0
    // xy = z2*B^2m + z1*B^m + z0
    // z2 = x1*y1, z0 = x0*y0, z1 = (x1+x0)(y1+y0)-z2-z0
    if (n <= 32) { //Basecase
      val z = Array.ofDim[Int](2 * n)
      var carry = 0L
      var tmp = 0L
      var xi = x(off) & MASK
      var j = 0
      while (j < n) {
        tmp = xi * (y(off + j) & MASK) + carry
        z(j) = tmp.toInt
        carry = tmp >>> 32
        j += 1
      }
      z(n) = carry.toInt
      var i = 1
      while (i < n) {
        xi = x(off + i) & MASK
        carry = 0
        var j = 0
        while (j < n) {
          tmp = xi * (y(off + j) & MASK) + (z(i + j) & MASK) + carry
          z(i + j) = tmp.toInt
          carry = tmp >>> 32
          j += 1
        }
        z(i + n) = carry.toInt
        i += 1
      }
      return z
    }

    val b = n >>> 1
    val z2 = kmul(x, y, off + b, n - b)
    val z0 = kmul(x, y, off, b)

    val x2 = Array.ofDim[Int](n - b + 1)
    val y2 = Array.ofDim[Int](n - b + 1)
    var carry = 0L
    var i = 0
    while (i < b) {
      carry = (x(off + b + i) & MASK) + (x(off + i) & MASK) + carry
      x2(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if ((n & 1) != 0) x2(b) = x(off + b + b)
    if (carry != 0) {
      x2(b) += 1
      if (x2(b) == 0) {
        x2(b + 1) += 1
      }
    }
    carry = 0
    i = 0
    while (i < b) {
      carry = (y(off + b + i) & MASK) + (y(off + i) & MASK) + carry
      y2(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if ((n & 1) != 0) {
      y2(b) = y(off + b + b)
    }
    if (carry != 0) {
      y2(b) += 1
      if (y2(b) == 0) {
        y2(b + 1) += 1
      }
    }

    val z1 = kmul(x2, y2, 0, n - b + (if (x2(n - b) != 0 || y2(n - b) != 0) 1 else 0))

    val z = Array.ofDim[Int](2 * n)
    System.arraycopy(z0, 0, z, 0, 2 * b) //Add z0
    System.arraycopy(z2, 0, z, b + b, 2 * (n - b)) //Add z2

    //Add z1
    carry = 0
    i = 0
    while (i < 2 * b) {
      carry = (z(i + b) & MASK) + (z1(i) & MASK) - (z2(i) & MASK) - (z0(i) & MASK) + carry
      z(i + b) = carry.toInt
      carry >>= 32
      i += 1
    }
    while (i < 2 * (n - b)) {
      carry = (z(i + b) & MASK) + (z1(i) & MASK) - (z2(i) & MASK) + carry
      z(i + b) = carry.toInt
      carry >>= 32
      i += 1
    }
    while (i < z1.length) {
      carry = (z(i + b) & MASK) + (z1(i) & MASK) + carry
      z(i + b) = carry.toInt
      carry >>= 32
      i += 1
    }
    if (carry != 0) {
      while ({ z(i + b) += 1; z(i + b) == 0 }) {
        i += 1
      }
    }

    return z
  }

  /**
   * Multiplies partial magnitude arrays x[off..off+n) and y[off...off+n) and returns the result.
   * Algorithm: Parallell Karatsuba
   *
   * @param x		The first magnitude array.
   * @param y		The second magnitude array.
   * @param off	The offset, where the first element is residing.
   * @param n		The length of each of the two partial arrays.
   * @param lim	The recursion depth up until which we will spawn new threads.
   * @param pool	Where spawn threads will be added and executed.
   * @throws		Various thread related exceptions.
   * @complexity	O(n^1.585)
   */
  @throws(classOf[Exception])
  private def pmul(x: Array[Int], y: Array[Int], off: Int, n: Int, lim: Int, pool: ExecutorService): Array[Int] = {
    val b = n >>> 1

    val left = pool.submit(new Callable[Array[Int]]() {
      @throws(classOf[Exception])
      def call(): Array[Int] = {
        return if (lim == 0) kmul(x, y, off, b) else pmul(x, y, off, b, lim - 1, pool)
      }
    })

    val right = pool.submit(new Callable[Array[Int]]() {
      @throws(classOf[Exception])
      def call(): Array[Int] = {
        return if (lim == 0) kmul(x, y, off + b, n - b) else pmul(x, y, off + b, n - b, lim - 1, pool)
      }
    })

    val x2 = Array.ofDim[Int](n - b + 1)
    val y2 = Array.ofDim[Int](n - b + 1)
    var carry = 0L
    var i = 0
    while (i < b) {
      carry = (x(off + b + i) & MASK) + (x(off + i) & MASK) + carry
      x2(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if ((n & 1) != 0) {
      x2(b) = x(off + b + b)
    }
    if (carry != 0) {
      if ({ x2(b) += 1; x2(b) == 0 }) {
        x2(b + 1) += 1
      }
    }
    carry = 0
    i = 0
    while (i < b) {
      carry = (y(off + b + i) & MASK) + (y(off + i) & MASK) + carry
      y2(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if ((n & 1) != 0) {
      y2(b) = y(off + b + b)
    }
    if (carry != 0) {
      if ({ y2(b) += 1; y2(b) == 0 }) {
        y2(b + 1) += 1
      }
    }

    val mid = pool.submit(new Callable[Array[Int]]() {
      @throws(classOf[Exception])
      def call(): Array[Int] = {
        if (lim == 0) {
          kmul(x2, y2, 0, n - b + (if (x2(n - b) != 0 || y2(n - b) != 0) 1 else 0))
        } else {
          pmul(x2, y2, 0, n - b + (if (x2(n - b) != 0 || y2(n - b) != 0) 1 else 0), lim - 1, pool)
        }
      }
    })

    val z = Array.ofDim[Int](2 * n)

    val z0 = left.get()
    System.arraycopy(z0, 0, z, 0, 2 * b)
    val z2 = right.get()
    System.arraycopy(z2, 0, z, b + b, 2 * (n - b))

    val z1 = mid.get()

    carry = 0
    i = 0
    while (i < 2 * b) {
      carry = (z(i + b) & MASK) + (z1(i) & MASK) - (z2(i) & MASK) - (z0(i) & MASK) + carry
      z(i + b) = carry.toInt
      carry >>= 32
      i += 1
    }
    while (i < 2 * (n - b)) {
      carry = (z(i + b) & MASK) + (z1(i) & MASK) - (z2(i) & MASK) + carry
      z(i + b) = carry.toInt
      carry >>= 32
      i += 1
    }
    while (i < z1.length) {
      carry = (z(i + b) & MASK) + (z1(i) & MASK) + carry
      z(i + b) = carry.toInt
      carry >>= 32
      i += 1
    }
    if (carry != 0) {
      while ({ z(i + b) += 1; z(i + b) == 0 }) {
        i += 1
      }
    }
    return z
  }
  /*** </Mul Helper> ***/
}

/**
 * <p>A class for arbitrary-precision integer arithmetic purely written in Java.</p>
 * <p>This class does what {@link java.math.BigInteger} doesn't.<br />
 * It is <b>faster</b>, and it is <b>mutable</b>!<br />
 * It supports <b>ints</b> and <b>longs</b> as parameters!<br />
 * It has a way faster {@link #toString()} method!<br />
 * It utilizes a faster multiplication algorithm for those nasty big numbers!</p>
 *
 * <p>Get it today! Because performance matters (and we like Java).</p>
 *
 * @author Simon Klein
 * @version 0.7
 */
final class BigInt private () extends Number with Comparable[BigInt] {
  import BigInt._

  /**
   * The sign of this number. 1 for positive numbers and -1 for negative
   * numbers. Zero can have either sign.
   */
  private var sign: Int = _

  /**
   * The number of digits of the number (in base 2^32).
   */
  private var len: Int = _

  /**
   * The digits of the number, i.e., the magnitude array.
   */
  private var mag: Array[Int] = Array[Int]()

  def this(_sign: Int, _mag: Array[Int], _len: Int) = {
    this()
    assign(_sign, _mag, _len)
  }

  /**
   * Creates a BigInt from the given parameters.
   * The contents of the input-array will be copied.
   *
   * @param sign	The sign of the number.
   * @param v		The magnitude of the number, the first position gives the least significant 8 bits.
   * @param len	The (first) number of entries of v that are considered part of the number.
   * @complexity	O(n)
   */
  def this(sign: Int, v: Array[Byte], _vlen: Int) = {
    this()
    var vlen = _vlen
    while (vlen > 1 && v(vlen - 1) == 0) {
      vlen -= 1
    }
    val dig = Array.ofDim[Int]((vlen + 3) / 4)
    assign(sign, v, vlen)
  }

  /**
   * Creates a BigInt from the given parameters.
   * The input-value will be interpreted as unsigned.
   *
   * @param sign	The sign of the number.
   * @param val	The magnitude of the number.
   * @complexity	O(1)
   */
  def this(sign: Int, value: Int) = {
    this()
    mag = Array.ofDim[Int](1)
    uassign(sign, value)
  }

  /**
   * Creates a BigInt from the given parameters.
   * The input-value will be interpreted as unsigned.
   *
   * @param sign	The sign of the number.
   * @param val	The magnitude of the number.
   * @complexity	O(1)
   */
  def this(sign: Int, value: Long) = {
    this()
    mag = Array.ofDim[Int](2)
    uassign(sign, value)
  }

  /**
   * Creates a BigInt from the given int.
   * The input-value will be interpreted a signed value.
   *
   * @param val	The value of the number.
   * @complexity	O(1)
   */
  def this(value: Int) = {
    this()
    mag = Array.ofDim[Int](1)
    assign(value)
  }

  /**
   * Creates a BigInt from the given long.
   * The input-value will be interpreted a signed value.
   *
   * @param val	The value of the number.
   * @complexity	O(1)
   */
  def this(value: Long) = {
    this()
    mag = Array.ofDim[Int](2)
    assign(value)
  }

  /**
   * Creates a BigInt from the given string.
   *
   * @param s	A string representing the number in decimal.
   * @complexity	O(n^2)
   */
  def this(s: String) = {
    this()
    assign(s)
  }

  /**
   * Creates a BigInt from the given char-array.
   *
   * @param s	A char array representing the number in decimal.
   * @complexity	O(n^2)
   */
  def this(s: Array[Char]) = {
    this()
    assign(s)
  }

  /*** <General Helper> ***/
  /**
   * Parses a part of a char array as an unsigned decimal number.
   *
   * @param s		A char array representing the number in decimal.
   * @param from	The index (inclusive) where we start parsing.
   * @param to		The index (exclusive) where we stop parsing.
   * @return		The parsed number.
   * @complexity	O(n)
   */
  private def parse(s: Array[Char], _from: Int, to: Int): Int = {
    var from = _from
    var res = s(from) - '0'
    while ({ from += 1; from } < to) {
      res = res * 10 + s(from) - '0'
    }
    res
  }

  /**
   * Multiplies this number and then adds something to it.
   * I.e. sets this = this*mul + add.
   *
   * @param mul	The value we multiply our number with, mul < 2^31.
   * @param add	The value we add to our number, add < 2^31.
   * @complexity	O(n)
   */
  private def mulAdd(mul: Int, add: Int) {
    var carry = 0L
    var i = 0
    while (i < len) {
      carry = mul * (mag(i) & MASK) + carry
      mag(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if (carry != 0) {
      mag(len) = carry.toInt
      len += 1
    }
    carry = (mag(0) & MASK) + add
    mag(0) = carry.toInt
    if ((carry >>> 32) != 0) {
      var i = 1
      while (i < len && { mag(i) += 1; mag(i) == 0 }) {
        i += 1
      }
      if (i == len) {
        mag(len) = 1 //Todo: realloc() for general case?
        len += 1
      }
    }
  }

  /**
   * Reallocates the magnitude array to one twice its size.
   *
   * @complexity	O(n)
   */
  private def realloc() {
    val res = Array.ofDim[Int](mag.length * 2)
    System.arraycopy(mag, 0, res, 0, len)
    mag = res
  }

  /**
   * Reallocates the magnitude array to one of the given size.
   *
   * @param newLen	The new size of the magnitude array.
   * @complexity	O(n)
   */
  private def realloc(newLen: Int) {
    val res = Array.ofDim[Int](newLen)
    System.arraycopy(mag, 0, res, 0, len)
    mag = res
  }
  /*** </General Helper> ***/

  /*** <General functions> ***/
  /**
   * Creates a copy of this number.
   *
   * @return The BigInt copy.
   * @complexity	O(n)
   */
  def copy() = {
    new BigInt(sign, java.util.Arrays.copyOf(mag, len), len)
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param The BigInt to copy/assign to this BigInt.
   * @complexity	O(n)
   */
  def assign(other: BigInt) {
    sign = other.sign
    assign(other.mag, other.len)
  }

  /**
   * Assigns the content of the given magnitude array and the length to this number.
   * The contents of the input will be copied.
   *
   * @param v		The new magnitude array content.
   * @param vlen	The length of the content, vlen > 0.
   * @complexity	O(n)
   */
  private def assign(v: Array[Int], vlen: Int) { //Todo: Better and more consistent naming.
    if (vlen > mag.length) {
      mag = Array.ofDim[Int](vlen + 2)
    }
    len = vlen
    System.arraycopy(v, 0, mag, 0, vlen)
  }

  /**
   * Assigns the given BigInt parameter to this number.
   * The input magnitude array will be used as is and not copied.
   *
   * @param sign	The sign of the number.
   * @param v		The magnitude of the number.
   * @param len 	The length of the magnitude array to be used.
   * @complexity	O(1)
   */
  def assign(sign: Int, v: Array[Int], len: Int) {
    this.sign = sign
    this.len = len
    mag = v
  }

  /**
   * Assigns the given BigInt parameter to this number.
   * Assumes no leading zeroes of the input-array, i.e. that v[vlen-1]!=0, except for the case when vlen==1.
   *
   * @param sign	The sign of the number.
   * @param v		The magnitude of the number.
   * @param vlen 	The length of the magnitude array to be used.
   * @complexity	O(n)
   */
  def assign(sign: Int, v: Array[Byte], vlen: Int) {
    len = (vlen + 3) / 4
    if (len > mag.length) {
      mag = Array.ofDim[Int](len + 2)
    }
    this.sign = sign
    var tmp = vlen / 4
    var j = 0
    var i = 0
    while (i < tmp) {
      mag(i) = v(j + 3) << 24 | (v(j + 2) & 0xFF) << 16 | (v(j + 1) & 0xFF) << 8 | v(j) & 0xFF
      i += 1
      j += 4
    }
    if (tmp != len) {
      tmp = v(j) & 0xFF
      j += 1
      if (j < vlen) {
        tmp |= (v(j) & 0xFF) << 8
        j += 1
        if (j < vlen) tmp |= (v(j) & 0xFF) << 16
      }
      mag(len - 1) = tmp
    }
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param s		A string representing the number in decimal.
   * @complexity	O(n^2)
   */
  def assign(s: String) {
    assign(s.toCharArray())
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param s		A char array representing the number in decimal.
   * @complexity	O(n^2)
   */
  def assign(s: Array[Char]) {
    sign = if (s(0) == '-') -1 else 1

    len = s.length + (sign - 1 >> 1)
    val alloc = if (len < 10) 1 else (len * 3402L >>> 10).toInt + 32 >>> 5 //3402 = bits per digit * 1024
    if (mag == null || alloc > mag.length) mag = Array.ofDim[Int](alloc)

    var j = len % 9
    if (j == 0) j = 9
    j -= (sign - 1 >> 1)

    mag(0) = parse(s, 0 - (sign - 1 >> 1), j)
    len = 1
    while (j < s.length) {
      mulAdd(1000000000, parse(s, j, { j += 9; j }))
    }
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param s		The sign of the number.
   * @param val	The magnitude of the number (will be intepreted as unsigned).
   * @complexity	O(1)
   */
  def uassign(s: Int, v: Int) {
    sign = s
    len = 1
    mag(0) = v
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param s		The sign of the number.
   * @param val	The magnitude of the number (will be intepreted as unsigned).
   * @complexity	O(1)
   */
  def uassign(s: Int, v: Long) {
    sign = s
    len = 2
    if (mag.length < 2) realloc(2)
    mag(0) = (v & MASK).toInt
    mag(1) = (v >>> 32).toInt
    if (mag(1) == 0) len -= 1
  }

  /**
   * Assigns the given non-negative number to this BigInt object.
   *
   * @param val	The number interpreted as unsigned.
   * @complexity	O(1)
   */
  def uassign(v: Int) {
    uassign(1, v)
  }

  /**
   * Assigns the given non-negative number to this BigInt object.
   *
   * @param val	The number interpreted as unsigned.
   * @complexity	O(1)
   */
  def uassign(v: Long) {
    uassign(1, v)
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param val	The number to be assigned.
   * @complexity	O(1)
   */
  def assign(v: Int) {
    uassign(if (v < 0) -1 else 1, if (v < 0) -v else v)
  }

  /**
   * Assigns the given number to this BigInt object.
   *
   * @param val	The number to be assigned.
   * @complexity	O(1)
   */
  def assign(v: Long) {
    uassign(if (v < 0) -1 else 1, if (v < 0) -v else v)
  }

  /**
   * Tells whether this number is zero or not.
   *
   * @return true if this number is zero, false otherwise
   * @complexity	O(1)
   */
  def isZero(): Boolean = {
    len == 1 && mag(0) == 0
  }

  /**
   * Sets this number to zero.
   *
   * @complexity	O(1)
   */
  private def setToZero() {
    mag(0) = 0
    len = 1
    sign = 1 //Remove?
  }

  /**
   * Compares the absolute value of this and the given number.
   *
   * @param a	The number to be compared with.
   * @return	-1 if the absolute value of this number is less, 0 if it's equal, 1 if it's greater.
   * @complexity	O(n)
   */
  def compareAbsTo(a: BigInt): Int = {
    if (len > a.len) return 1
    if (len < a.len) return -1
    var i = len - 1
    while (i >= 0) {
      if (mag(i) != a.mag(i)) {
        if ((mag(i) & MASK) > (a.mag(i) & MASK)) {
          return 1
        } else {
          return -1
        }
      }
      i -= 1
    }
    return 0
  }

  /**
   * Compares the value of this and the given number.
   *
   * @param a	The number to be compared with.
   * @return	-1 if the value of this number is less, 0 if it's equal, 1 if it's greater.
   * @complexity	O(n)
   */
  def compareTo(a: BigInt): Int = {
    if (sign < 0) {
      if (a.sign < 0 || a.isZero()) {
        return -compareAbsTo(a)
      }
      return -1
    }
    if (a.sign > 0 || a.isZero()) {
      return compareAbsTo(a)
    }
    return 1
  }

  /**
   * Tests equality of this number and the given one.
   *
   * @param a	The number to be compared with.
   * @return	true if the two numbers are equal, false otherwise.
   * @complexity	O(n)
   */
  def equals(a: BigInt): Boolean = {
    if (len != a.len) return false
    if (isZero() && a.isZero()) return true
    if ((sign ^ a.sign) < 0) return false //In case definition of sign would change...
    var i = 0
    while (i < len) {
      if (mag(i) != a.mag(i)) {
        return false
      }
      i += 1
    }
    return true
  }

  /**
   * {@inheritDoc}
   */
  override def equals(o: Any): Boolean = { //Todo: Equality on other Number objects?
    if (o.isInstanceOf[BigInt]) return equals(o.asInstanceOf[BigInt])
    return false
  }

  /**
   * {@inheritDoc}
   */
  override def hashCode(): Int = {
    var hash = 0 //Todo: Opt and improve.
    var i = 0
    while (i < len) {
      hash = (31 * hash + (mag(i) & MASK)).toInt
      i += 1
    }
    return sign * hash //relies on 0 --> hash==0.
  }
  /*** </General functions> ***/

  /*** <Number Override> ***/
  /**
   * {@inheritDoc}
   * Returns this BigInt as a {@code byte}.
   *
   * @return {@code sign * (this & 0x7F)}
   */
  override def byteValue(): Byte = {
    (sign * (mag(0) & 0x7F)).toByte
  }

  /**
   * {@inheritDoc}
   * Returns this BigInt as a {@code short}.
   *
   * @return {@code sign * (this & 0x7FFF)}
   */
  override def shortValue(): Short = {
    (sign * (mag(0) & 0x7FFF)).toShort
  }

  /**
   * {@inheritDoc}
   * Returns this BigInt as an {@code int}.
   *
   * @return {@code sign * (this & 0x7FFFFFFF)}
   */
  override def intValue(): Int = {
    sign * (mag(0) & 0x7FFFFFFF) //relies on that sign always is either 1/-1.
  }

  /**
   * {@inheritDoc}
   * Returns this BigInt as a {@code long}.
   *
   * @return {@code sign * (this & 0x7FFFFFFFFFFFFFFF)}
   */
  override def longValue(): Long = {
    if (len == 1) sign * (mag(0) & MASK) else sign * ((mag(1) & 0x7FFFFFFFL) << 32 | (mag(0) & MASK))
  }

  /**
   * {@inheritDoc}
   * Returns this BigInt as a {@code float}.
   *
   * @return the most significant 24 bits in the mantissa (the highest order bit obviously being implicit),
   * 	the exponent value which will be consistent for {@code BigInt}s up to 128 bits (should it not fit it'll be calculated modulo 256),
   * 	and the sign bit set if this number is negative.
   */
  override def floatValue(): Float = {
    val s = java.lang.Integer.numberOfLeadingZeros(mag(len - 1))
    if (len == 1 && s >= 8) return sign * mag(0)

    var bits = mag(len - 1) //Mask out the 24 MSBits.
    if (s <= 8) bits >>>= 8 - s
    else bits = bits << s - 8 | mag(len - 2) >>> 32 - (s - 8) //s-8==additional bits we need.
    bits = (bits ^ (1L << 23)).toInt //The leading bit is implicit, cancel it out.

    val exp = (((32 - s + 32L * (len - 1)) - 1 + 127) & 0xFF).toInt
    bits |= exp << 23 //Add exponent.
    bits |= sign & (1 << 31) //Add sign-bit.

    return java.lang.Float.intBitsToFloat(bits)
  }

  /**
   * {@inheritDoc}
   * Returns this BigInt as a {@code double}.
   *
   * @return the most significant 53 bits in the mantissa (the highest order bit obviously being implicit),
   * 	the exponent value which will be consistent for {@code BigInt}s up to 1024 bits (should it not fit it'll be calculated modulo 2048),
   * 	and the sign bit set if this number is negative.
   */
  override def doubleValue(): Double = {
    if (len == 1) return sign * (mag(0) & MASK)

    val s = java.lang.Integer.numberOfLeadingZeros(mag(len - 1))
    if (len == 2 && 32 - s + 32 <= 53) return sign * (mag(1).toLong << 32 | (mag(0) & MASK))

    var bits = mag(len - 1).toLong << 32 | (mag(len - 2) & MASK) //Mask out the 53 MSBits.
    if (s <= 11) bits >>>= 11 - s
    else bits = bits << s - 11 | mag(len - 3) >>> 32 - (s - 11) //s-11==additional bits we need.
    bits ^= 1L << 52 //The leading bit is implicit, cancel it out.

    val exp = ((32 - s + 32L * (len - 1)) - 1 + 1023) & 0x7FF
    bits |= exp << 52 //Add exponent.
    bits |= sign.toLong & (1L << 63) //Add sign-bit.

    java.lang.Double.longBitsToDouble(bits)
  }

  /*** </Number Override> ***/

  /*** <Unsigned Int Num> ***/
  /**
   * Increases the magnitude of this number.
   *
   * @param a	The amount of the increase (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  private def uaddMag(a: Int) {
    val tmp = (mag(0) & MASK) + (a & MASK)
    mag(0) = tmp.toInt
    if ((tmp >>> 32) != 0) {
      var i = 1
      while (i < len && { mag(i) += 1; mag(i) == 0 }) {
        i += 1
      }
      if (i == len) {
        if (len == mag.length) {
          realloc()
        }
        mag(len) = 1
        len += 1
      }
    }
  }

  /**
   * Decreases the magnitude of this number.
   * If s > this behaviour is undefined.
   *
   * @param s	The amount of the decrease (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  private def usubMag(s: Int) {
    val dif = (mag(0) & MASK) - (s & MASK)
    mag(0) = dif.toInt
    if ((dif >> 32) != 0) {
      var i = 1
      while (mag(i) == 0) {
        mag(i) -= 1
        i += 1
      }
      mag(i) -= 1
      if (mag(i) == 0 && i + 1 == len) len -= 1
    }
  }

  /**
   * Adds an unsigned int to this number.
   *
   * @param a	The amount to add (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  def uadd(a: Int) {
    if (sign < 0) {
      if (len > 1 || (mag(0) & MASK) > (a & MASK)) {
        usubMag(a)
        return
      }
      sign = 1
      mag(0) = a - mag(0)
      return
    }

    uaddMag(a)
  }

  /**
   * Subtracts an unsigned int from this number.
   *
   * @param s	The amount to subtract (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  def usub(s: Int) {
    if (sign < 0) {
      uaddMag(s)
      return
    }
    if (len == 1 && (mag(0) & MASK) < (s & MASK)) {
      sign = -1
      mag(0) = s - mag(0)
      return
    }

    usubMag(s)
  }

  /**
   * Multiplies this number with an unsigned int.
   *
   * @param mul	The amount to multiply (treated as unsigned).
   * @complexity	O(n)
   */
  def umul(mul: Int) { //mul is interpreted as unsigned
    if (mul == 0) {
      setToZero()
      return
    } //To be removed?

    var carry = 0L
    val m = mul & MASK
    var i = 0
    while (i < len) {
      carry = (mag(i) & MASK) * m + carry
      mag(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if (carry != 0) {
      if (len == mag.length) realloc()
      mag(len) = carry.toInt
      len += 1
    }
  }

  /**
   * Divides this number with an unsigned int and returns the remainder.
   *
   * @param div	The amount to divide with (treated as unsigned).
   * @return		The absolute value of the remainder as an unsigned int.
   * @complexity	O(n)
   */
  def udiv(div: Int): Int = { //returns the unsigned remainder!
    if (div < 0) return safeUdiv(div)
    else return unsafeUdiv(div)
  }

  // Assumes div > 0.
  private def unsafeUdiv(div: Int): Int = {
    val d = div & MASK
    var rem = 0L
    var i = len - 1
    while (i >= 0) {
      rem <<= 32
      rem = rem + (mag(i) & MASK)
      mag(i) = (rem / d).toInt //Todo: opt?
      rem = rem % d
      i -= 1
    }
    if (mag(len - 1) == 0 && len > 1) len -= 1
    if (len == 1 && mag(0) == 0) sign = 1
    return rem.toInt
  }

  // Assumes div < 0.
  private def safeUdiv(div: Int): Int = {
    val d = div & MASK
    val hbit = Long.MinValue
    var hq = (hbit - 1) / d
    if (hq * d + d == hbit) hq += 1
    val hrem = hbit - hq * d
    var rem = 0L
    var i = len - 1
    while (i >= 0) {
      rem = (rem << 32) + (mag(i) & MASK)
      val q = (hq & rem >> 63) + ((rem & hbit - 1) + (hrem & rem >> 63)) / d
      rem = rem - q * d
      mag(i) = q.toInt
      i -= 1
    }
    if (mag(len - 1) == 0 && len > 1) len -= 1
    if (len == 1 && mag(0) == 0) sign = 1
    return rem.toInt
  }

  /**
   * Modulos this number with an unsigned int.
   * I.e. sets this number to this % mod.
   *
   * @param mod	The amount to modulo with (treated as unsigned).
   * @complexity	O(n)
   */
  def urem(mod: Int) {
    if (mod < 0) safeUrem(mod) else unsafeUrem(mod)
  }

  // Assumes mod > 0.
  private def unsafeUrem(mod: Int) {
    var rem = 0L
    var d = mod & MASK
    var i = len - 1
    while (i >= 0) {
      rem <<= 32
      rem = (rem + (mag(i) & MASK)) % d
      i -= 1
    }
    len = 1
    mag(0) = rem.toInt
    if (mag(0) == 0) sign = 1
  }

  // Assumes mod < 0.
  private def safeUrem(mod: Int) {
    val d = mod & MASK
    val hbit = Long.MinValue
    // Precompute hrem = (1<<63) % d
    // I.e. the remainder caused by the highest bit.
    var hrem = (hbit - 1) % d
    hrem += 1
    if (hrem == d) hrem = 0
    var rem = 0L
    var i = len - 1
    while (i >= 0) {
      rem = (rem << 32) + (mag(i) & MASK)
      // Calculate rem %= d.
      // Do this by calculating the lower 63 bits and highest bit separately.
      // The highest bit remainder only gets added if it's set.
      rem = ((rem & hbit - 1) + (hrem & rem >> 63)) % d
      // The addition is safe and cannot overflow.
      // Because hrem < 2^32 and there's at least one zero bit in [62,32] if bit 63 is set.
      i -= 1
    }
    len = 1
    mag(0) = rem.toInt
    if (mag(0) == 0) sign = 1
  }
  /*** </Unsigned Int Num> ***/

  /*** <Unsigned Long Num> ***/
  /**
   * Increases the magnitude of this number.
   *
   * @param a	The amount of the increase (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  private def uaddMag(a: Long) {
    if (mag.length <= 2) {
      realloc(3)
      len = 2
    }

    val ah = a >>> 32
    val al = a & MASK
    var carry = (mag(0) & MASK) + al
    mag(0) = carry.toInt
    carry >>>= 32
    carry = (mag(1) & MASK) + ah + carry
    mag(1) = carry.toInt
    if ((carry >> 32) != 0) {
      var i = 2
      while (i < len && { mag(i) += 1; mag(i) == 0 }) {
        i += 1
      }
      if (i == len) {
        if (len == mag.length) {
          realloc()
        }
        mag(len) = 1
        len += 1
      }
    } else if (len == 2 && mag(1) == 0) len -= 1
  }

  /**
   * Decreases the magnitude of this number.
   * If s > this behaviour is undefined.
   *
   * @param s	The amount of the decrease (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  private def usubMag(s: Long) {
    val sh = s >>> 32
    val sl = s & MASK
    var dif = (mag(0) & MASK) - sl
    mag(0) = dif.toInt
    dif >>= 32
    dif = (mag(1) & MASK) - sh + dif
    mag(1) = dif.toInt
    if ((dif >> 32) != 0) {
      var i = 2
      while (mag(i) == 0) {
        mag(i) -= 1
        i += 1
      }
      mag(i) -= 1
      if (mag(i) == 0 && i + 1 == len) len -= 1
    }
    if (len == 2 && mag(1) == 0) len -= 1
  }

  /**
   * Adds an unsigned long to this number.
   *
   * @param a	The amount to add (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  def uadd(a: Long) { //Refactor? Similar to usub.
    if (sign < 0) {
      val ah = a >>> 32
      val al = a & MASK
      if (len > 2 || len == 2 && ((mag(1) & MASK) > ah || (mag(1) & MASK) == ah && (mag(0) & MASK) >= al) || ah == 0 && (mag(0) & MASK) >= al) {
        usubMag(a)
        return
      }
      if (mag.length == 1) realloc(2)
      if (len == 1) {
        mag(len) = 0
        len += 1
      }
      var dif = al - (mag(0) & MASK)
      mag(0) = dif.toInt
      dif >>= 32
      dif = ah - (mag(1) & MASK) + dif
      mag(1) = dif.toInt
      //dif>>32 != 0 should be impossible
      if (dif == 0) len -= 1
      sign = 1
    } else uaddMag(a)
  }

  /**
   * Subtracts an unsigned long from this number.
   *
   * @param a	The amount to subtract (treated as unsigned).
   * @complexity	O(n)
   * @amortized	O(1)
   */
  def usub(a: Long) { //Fix parameter name
    if (sign > 0) {
      val ah = a >>> 32
      val al = a & MASK
      if (len > 2 || len == 2 && ((mag(1) & MASK) > ah || (mag(1) & MASK) == ah && (mag(0) & MASK) >= al) || ah == 0 && (mag(0) & MASK) >= al) {
        usubMag(a)
        return
      }
      if (mag.length == 1) realloc(2)
      if (len == 1) {
        mag(len) = 0
        len += 1
      }
      var dif = al - (mag(0) & MASK)
      mag(0) = dif.toInt
      dif >>= 32
      dif = ah - (mag(1) & MASK) + dif
      mag(1) = dif.toInt
      //dif>>32 != 0 should be impossible
      if (dif == 0) len -= 1
      sign = -1
    } else uaddMag(a)
  }

  /**
   * Multiplies this number with an unsigned long.
   *
   * @param mul	The amount to multiply (treated as unsigned).
   * @complexity	O(n)
   */
  def umul(mul: Long) {
    if (mul == 0) {
      setToZero()
      return
    }
    if (len + 2 >= mag.length) realloc(2 * len + 1)

    val mh = mul >>> 32
    val ml = mul & MASK
    var carry = 0L
    var next = 0L
    var tmp = 0L
    var i = 0
    while (i < len) {
      carry = carry + next //Could this overflow?
      tmp = (mag(i) & MASK) * ml
      next = (mag(i) & MASK) * mh
      mag(i) = (tmp + carry).toInt
      carry = (tmp >>> 32) + (carry >>> 32) + ((tmp & MASK) + (carry & MASK) >>> 32)
      i += 1
    }
    carry = carry + next
    mag(len) = carry.toInt
    len += 1
    mag(len) = (carry >>> 32).toInt
    len += 1

    while (len > 1 && mag(len - 1) == 0) len -= 1
  }

  /**
   * Divides this number with an unsigned long and returns the remainder.
   *
   * @param div	The amount to divide with (treated as unsigned).
   * @return		The absolute value of the remainder as an unsigned long.
   * @complexity	O(n)
   */
  def udiv(value: Long): Long = { //Adaption of general div to long.
    if (value == (value & MASK)) {
      return udiv(value.toInt) & MASK
    }
    if (len == 1) {
      val tmp = mag(0) & MASK
      setToZero()
      return tmp
    }

    val s = java.lang.Integer.numberOfLeadingZeros((value >>> 32).toInt)
    val dh = value >>> 32 - s
    val dl = (value << s) & MASK
    val hbit = Long.MinValue

    var u2 = 0L
    var u1: Long = mag(len - 1) >>> 32 - s
    var u0: Long = (mag(len - 1) << s | mag(len - 2) >>> 32 - s) & MASK
    if (s == 0) {
      u1 = 0
      u0 = mag(len - 1) & MASK
    }
    var j = len - 2
    while (j >= 0) {
      u2 = u1
      u1 = u0
      u0 = if (s > 0 && j > 0) (mag(j) << s | mag(j - 1) >>> 32 - s) & MASK else (mag(j) << s) & MASK

      var k = (u2 << 32) + u1 //Unsigned division is a pain in the ass! ='(
      var qhat = (k >>> 1) / dh << 1
      var t = k - qhat * dh
      if (t + hbit >= dh + hbit) {
        qhat += 1 // qhat = (u[j+n]*b + u[j+n-1])/v[n-1]
      }
      var rhat = k - qhat * dh

      var break = false
      while (!break && (qhat + hbit >= (1L << 32) + hbit || qhat * dl + hbit > (rhat << 32) + u0 + hbit)) { //Unsigned comparison.
        qhat -= 1
        rhat = rhat + dh
        if (rhat + hbit >= (1L << 32) + hbit) {
          break = true
        }
      }

      // Multiply and subtract. Unfolded loop.
      var p = qhat * dl
      t = u0 - (p & MASK)
      u0 = t & MASK
      k = (p >>> 32) - (t >> 32)
      p = qhat * dh
      t = u1 - k - (p & MASK)
      u1 = t & MASK
      k = (p >>> 32) - (t >> 32)
      t = u2 - k
      u2 = t & MASK

      mag(j) = qhat.toInt // Store quotient digit. If we subtracted too much, add back.
      if (t < 0) {
        mag(j) -= 1 //Unfolded loop.
        t = u0 + dl
        u0 = t & MASK
        t >>>= 32
        t = u1 + dh + t
        u1 = t & MASK
        t >>>= 32
        u2 += t & MASK
      }
      j -= 1
    }

    len -= 1
    mag(len) = 0
    if (len > 1 && mag(len - 1) == 0) {
      len -= 1
    }

    val tmp = u1 << 32 - s | u0 >>> s
    if (s == 0) tmp else u2 << 64 - s | tmp
  }

  /**
   * Modulos this number with an unsigned long.
   * I.e. sets this number to this % mod.
   *
   * @param mod	The amount to modulo with (treated as unsigned).
   * @complexity	O(n)
   */
  def urem(mod: Long) {
    val rem = udiv(mod) //todo: opt?
    len = 2
    mag(0) = rem.toInt
    if (rem == (rem & MASK)) {
      len -= 1
      return
    } //if(dig[0]==0) sign = 1
    mag(1) = (rem >>> 32).toInt
  }
  /*** </Unsigned Long Num> ***/

  /*** <Signed Small Num> ***/
  /**
   * Adds an int to this number.
   *
   * @param add	The amount to add.
   * @complexity	O(n)
   */
  def add(add: Int) { //Has not amortized O(1) due to the risk of alternating +1 -1 on continuous sequence of 1-set bits.
    if (add < 0) usub(-add) else uadd(add)
  }

  /**
   * Subtracts an int from this number.
   *
   * @param sub	The amount to subtract.
   * @complexity	O(n)
   */
  def sub(sub: Int) {
    if (sub < 0) uadd(-sub) else usub(sub)
  }

  /**
   * Multiplies this number with an int.
   *
   * @param mul	The amount to multiply with.
   * @complexity	O(n)
   */
  def mul(mul: Int) {
    if (isZero()) return //Remove?
    if (mul < 0) {
      sign = -sign
      umul(-mul)
    } else {
      umul(mul)
    }
  }

  /**
   * Divides this number with an int.
   *
   * @param div	The amount to divide with.
   * @complexity	O(n)
   * @return		the signed remainder.
   */
  def div(div: Int): Int = {
    if (isZero()) return 0 //Remove?
    if (div < 0) {
      sign = -sign
      return -sign * udiv(-div)
    }
    return sign * udiv(div)
  }

  // --- Long SubSection ---
  /**
   * Adds a long to this number.
   *
   * @param add	The amount to add.
   * @complexity	O(n)
   */
  def add(add: Long) {
    if (add < 0) usub(-add) else uadd(add)
  }

  /**
   * Subtracts a long from this number.
   *
   * @param sub	The amount to subtract.
   * @complexity	O(n)
   */
  def sub(sub: Long) {
    if (sub < 0) uadd(-sub) else usub(sub)
  }

  /**
   * Multiplies this number with a long.
   *
   * @param mul	The amount to multiply with.
   * @complexity	O(n)
   */
  def mul(mul: Long) {
    if (isZero()) return //remove?
    if (mul < 0) {
      sign = -sign
      umul(-mul)
    } else {
      umul(mul)
    }
  }

  /**
   * Divides this number with a {@code long}.
   *
   * @param div	The amount to divide with.
   * @complexity	O(n)
   * @return		the signed remainder.
   */
  def div(div: Long): Long = {
    if (isZero()) return 0L //Remove?
    if (div < 0) {
      sign = -sign
      return -sign * udiv(-div)
    }
    return sign * udiv(div)
  }
  /*** </Signed Small Num> ***/

  /*** <Big Num Helper> ***/
  /**
   * Increases the magnitude of this number by the given magnitude array.
   *
   * @param v		The magnitude array of the increase.
   * @param vlen	The length (number of digits) of the increase.
   * @complexity	O(n)
   */
  private def addMag(_v: Array[Int], _vlen: Int) {
    var v = _v
    var vlen = _vlen
    var ulen = len
    var u = mag //ulen <= vlen
    if (vlen < ulen) {
      u = v
      v = mag
      ulen = vlen
      vlen = len
    }
    if (vlen > mag.length) realloc(vlen + 1)

    var carry = 0L
    var i = 0
    while (i < ulen) {
      carry = (u(i) & MASK) + (v(i) & MASK) + carry
      mag(i) = carry.toInt
      carry >>>= 32
      i += 1
    }
    if (vlen > len) {
      System.arraycopy(v, len, mag, len, vlen - len)
      len = vlen
    }
    if (carry != 0) { //carry==1

      while (i < len && { mag(i) += 1; mag(i) == 0 }) {
        i += 1
      }
      if (i == len) { //vlen==len
        if (len == mag.length) realloc()
        mag(len) = 1
        len += 1
      }
    }
  }

  /**
   * Decreases the magnitude of this number by the given magnitude array.
   * Behaviour is undefined if u > |this|.
   *
   * @param u	The magnitude array of the decrease.
   * @param vlen	The length (number of digits) of the decrease.
   * @complexity	O(n)
   */
  private def subMag(u: Array[Int], ulen: Int) {
    val vlen = len
    val v = mag //ulen <= vlen

    //Assumes vlen=len and v=dig
    var dif = 0L
    var i = 0
    while (i < ulen) {
      dif = (v(i) & MASK) - (u(i) & MASK) + dif
      mag(i) = dif.toInt
      dif >>= 32
      i += 1
    }
    if (dif != 0) {
      while (mag(i) == 0) {
        mag(i) -= 1
        i += 1
      }
      mag(i) -= 1
      if (mag(i) == 0 && i + 1 == len) len = ulen
    }
    while (len > 1 && mag(len - 1) == 0) len -= 1
  }
  /*** </Big Num Helper> ***/

  /*** <Big Num> ***/
  /**
   * Adds a BigInt to this number.
   *
   * @param a	The number to add.
   * @complexity	O(n)
   */
  def add(a: BigInt) {
    if (sign == a.sign) {
      addMag(a.mag, a.len)
      return
    }
    if (compareAbsTo(a) >= 0) {
      subMag(a.mag, a.len)
      //if(len==1 && dig[0]==0) sign = 1
      return
    }

    val v = a.mag
    val vlen = a.len
    if (mag.length < vlen) realloc(vlen + 1)

    sign = -sign
    var dif = 0L
    var i = 0
    while (i < len) {
      dif = (v(i) & MASK) - (mag(i) & MASK) + dif
      mag(i) = dif.toInt
      dif >>= 32
      i += 1
    }
    if (vlen > len) {
      System.arraycopy(v, len, mag, len, vlen - len)
      len = vlen
    }
    if (dif != 0) {
      while (i < vlen && mag(i) == 0) {
        mag(i) -= 1
        i += 1
      }
      mag(i) -= 1
      if (mag(i) == 0 && i + 1 == len) len -= 1
    }
    //if(i==vlen) should be impossible
  }

  /**
   * Subtracts a BigInt from this number.
   *
   * @param a	The number to subtract.
   * @complexity	O(n)
   */
  def sub(a: BigInt) { //Fix naming.
    if (sign != a.sign) {
      addMag(a.mag, a.len)
      return
    }
    if (compareAbsTo(a) >= 0) {
      subMag(a.mag, a.len)
      //if(len==1 && dig[0]==0) sign = 1
      return
    }

    val v = a.mag
    val vlen = a.len
    if (mag.length < vlen) realloc(vlen + 1)

    sign = -sign
    var dif = 0L
    var i = 0
    while (i < len) {
      dif = (v(i) & MASK) - (mag(i) & MASK) + dif
      mag(i) = dif.toInt
      dif >>= 32
      i += 1
    }
    if (vlen > len) {
      System.arraycopy(v, len, mag, len, vlen - len)
      len = vlen
    }
    if (dif != 0) {
      while (i < vlen && mag(i) == 0) {
        mag(i) -= 1
        i += 1
      }
      mag(i) -= 1
      if (mag(i) == 0 && i + 1 == len) len -= 1
    }
    //if(i==vlen) should be impossible
  }

  // --- Multiplication SubSection ---
  /**
   * Multiplies this number by the given BigInt.
   * Chooses the appropriate algorithm with regards to the size of the numbers.
   *
   * @param mul	The number to multiply with.
   * @complexity	O(n^2) - O(n log n)
   */
  def mul(mul: BigInt) {
    if (isZero()) return
    else if (mul.isZero()) setToZero()
    else if (mul.len <= 2 || len <= 2) {
      sign *= mul.sign
      if (mul.len == 1) umul(mul.mag(0))
      else if (len == 1) {
        val tmp = mag(0)
        assign(mul.mag, mul.len)
        umul(tmp)
      } else if (mul.len == 2) umul(mul.mag(1).toLong << 32 | (mul.mag(0) & MASK))
      else {
        val tmp = mag(1).toLong << 32 | (mag(0) & MASK)
        assign(mul.mag, mul.len)
        umul(tmp)
      }
    } else if (len < 128 || mul.len < 128 || len.toLong * mul.len < 1000000) smallMul(mul) //Remove overhead?
    else if (math.max(len, mul.len) < 20000) karatsuba(mul, false) //Tune thresholds and remove hardcode.
    else karatsuba(mul, true)
  }

  /**
   * Multiplies this number by the given (suitably small) BigInt.
   * Uses a quadratic algorithm which is often suitable for smaller numbers.
   *
   * @param mul	The number to multiply with.
   * @complexity	O(n^2)
   */
  def smallMul(mul: BigInt) {
    if (isZero()) return //Remove?
    if (mul.isZero()) {
      setToZero()
      return
    }

    sign *= mul.sign

    var ulen = len
    var vlen = mul.len
    var u = mag
    var v = mul.mag //ulen <= vlen
    if (vlen < ulen) {
      u = v
      v = mag
      ulen = vlen
      vlen = len
    }

    val res = naiveMul(u, ulen, v, vlen) //Todo remove function overhead.

    mag = res
    len = res.length
    if (res(len - 1) == 0) len -= 1
  }

  /**
   * Multiplies this number by the given BigInt using the Karatsuba algorithm.
   *
   * @param mul		The number to multiply with.
   * @complexity		O(n^1.585)
   */
  def karatsuba(mul: BigInt) { //Fix naming?
    karatsuba(mul, false)
  }

  /**
   * Multiplies this number by the given BigInt using the Karatsuba algorithm.
   * The caller can choose to use a parallel version which is more suitable for larger numbers.
   *
   * @param mul		The number to multiply with.
   * @param parallel	true if we should try to parallelize the algorithm, false if we shouldn't.
   * @complexity		O(n^1.585)
   */
  def karatsuba(mul: BigInt, parallel: Boolean) { //Not fully tested on small numbers... Fix naming?
    if (mul.mag.length < len) mul.realloc(len)
    else if (mag.length < mul.len) realloc(mul.len)

    if (mul.len < len) {
      var i = mul.len
      while (i < len) {
        mul.mag(i) = 0
        i += 1
      }
    }
    if (len < mul.len) {
      var i = len
      while (i < mul.len) {
        mag(i) = 0
        i += 1
      }
    }

    val mlen = math.max(len, mul.len)
    var res: Array[Int] = null
    if (!parallel) res = kmul(mag, mul.mag, 0, mlen)
    else {
      val pool = Executors.newFixedThreadPool(12)
      try {
        res = pmul(mag, mul.mag, 0, mlen, 1, pool)
      } catch {
        case e: Exception => System.err.println(e)
      }
      pool.shutdown()
    }

    len = len + mul.len
    while (res(len - 1) == 0) {
      len -= 1
    }
    mag = res
    sign *= mul.sign
  }

  // --- Division and Remainder SubSection ---
  /**
   * Divides this number by the given BigInt.
   * Division by zero is undefined.
   *
   * @param div	The number to divide with.
   * @complexity	O(n^2)
   */
  def div(value: BigInt) {
    if (value.len == 1) {
      sign *= value.sign
      udiv(value.mag(0))
      return
    }

    var tmp = compareAbsTo(value)
    if (tmp < 0) {
      setToZero()
      return
    }
    if (tmp == 0) {
      uassign(1, sign * value.sign)
      return
    }

    val q = Array.ofDim[Int](len - value.len + 1)
    if (len == mag.length) realloc(len + 1) //We need an extra slot.
    div(mag, value.mag, len, value.len, q)

    mag = q
    len = q.length
    while (len > 1 && mag(len - 1) == 0) {
      len -= 1
    }
    sign *= value.sign
  }

  /**
   * Sets this number to the remainder r satisfying q*div + r = this, where q = floor(this/div).
   *
   * @param div	The number to use in the division causing the remainder.
   * @complexity	O(n^2)
   */
  def rem(value: BigInt) {
    // -7/-3 = 2, 2*-3 + -1
    // -7/3 = -2, -2*3 + -1
    // 7/-3 = -2, -2*-3 + 1
    // 7/3 = 2, 2*3 + 1
    if (value.len == 1) {
      urem(value.mag(0))
      return
    }

    var tmp = compareAbsTo(value)
    if (tmp < 0) return
    if (tmp == 0) {
      setToZero()
      return
    }

    val q = Array.ofDim[Int](len - value.len + 1)
    if (len == mag.length) realloc(len + 1) //We need an extra slot.
    div(mag, value.mag, len, value.len, q)

    len = value.len
    while (mag(len - 1) == 0) {
      len -= 1
    }
  }

  /**
   * Modulo operation between this BigInt and parameter.
   * Similar to rem method of BigInt except it will always return positive values
   *
   * @param div
   */
  @throws(classOf[ArithmeticException])
  def mod(div: BigInt) {
    if (div.compareTo(new BigInt(0)) <= 0) {
      throw new ArithmeticException("Divisor must be > 0");
    }
    rem(div)
    if (sign < 0) {
      add(div)
    }
  }

  /**
   * Divides this number by the given BigInt and returns the remainder.
   * Division by zero is undefined.
   *
   * @param div	The number to divide with.
   * @return		The remainder.
   * @complexity	O(n^2)
   */
  def divRem(value: BigInt): BigInt = {
    var tmp = sign
    if (value.len == 1) {
      sign *= value.sign
      return new BigInt(tmp, udiv(value.mag(0)))
    }

    tmp = compareAbsTo(value)
    if (tmp < 0) {
      val cpy = new BigInt(sign, mag, len)
      mag = Array.ofDim[Int](2)
      len = 1 //setToZero()
      return cpy
    }
    if (tmp == 0) {
      val newSign = sign + value.sign
      uassign(1, newSign)
      sign = newSign

      return new BigInt(1, 0)
    }

    val q = Array.ofDim[Int](len - value.len + 1)
    if (len == mag.length) realloc(len + 1) //We need an extra slot.
    div(mag, value.mag, len, value.len, q)

    val r = mag
    mag = q
    len = q.length
    while (len > 1 && mag(len - 1) == 0) {
      len -= 1
    }

    tmp = value.len
    while (tmp > 1 && r(tmp - 1) == 0) {
      tmp -= 1
    }
    sign *= value.sign
    return new BigInt(sign / value.sign, r, tmp)
  }

  /**
   * Divides the first magnitude u[0..m) by v[0..n) and stores the resulting quotient in q.
   * The remainder will be stored in u, so u will be destroyed.
   * u[] must have room for an additional element, i.e. u[m] is a legal access.
   *
   * @param u	The first magnitude array, the dividend.
   * @param v	The second magnitude array, the divisor.
   * @param m	The length of the first array.
   * @param n	The length of the second array.
   * @param q	An array of length at least n-m+1 where the quotient will be stored.
   * @complexity	O(m*n)
   */
  private def div(u: Array[Int], v: Array[Int], m: Int, n: Int, q: Array[Int]) {
    //Hacker'sDelight's implementation of Knuth's Algorithm D
    val b = 1L << 32 // Number base (32 bits).
    var qhat = 0L // Estimated quotient digit.
    var rhat = 0L // A remainder.
    var p = 0L // Product of two digits.

    var s = 0
    var i = 0
    var j = 0
    var t = 0L
    var k = 0L

    // Normalize by shifting v left just enough so that
    // its high-order bit is on, and shift u left the
    // same amount.  We may have to append a high-order
    // digit on the dividend, we do that unconditionally.

    s = java.lang.Integer.numberOfLeadingZeros(v(n - 1))
    if (s > 0) { //In Java (x<<32)==(x<<0) so...
      var i = n - 1
      while (i > 0) {
        v(i) = (v(i) << s) | (v(i - 1) >>> 32 - s)
        i -= 1
      }
      v(0) = v(0) << s

      u(m) = u(m - 1) >>> 32 - s
      i = m - 1
      while (i > 0) {
        u(i) = (u(i) << s) | (u(i - 1) >>> 32 - s)
        i -= 1
      }
      u(0) = u(0) << s
    }

    val dh = v(n - 1) & MASK
    val dl = v(n - 2) & MASK
    val hbit = Long.MinValue

    j = m - n
    while (j >= 0) { //Main loop
      // Compute estimate qhat of q[j].
      k = u(j + n) * b + (u(j + n - 1) & MASK) //Unsigned division is a pain in the ass! ='(
      qhat = (k >>> 1) / dh << 1
      t = k - qhat * dh
      if (t + hbit >= dh + hbit) qhat += 1 // qhat = (u[j+n]*b + u[j+n-1])/v[n-1]
      rhat = k - qhat * dh

      var break = false
      while (!break && (qhat + hbit >= b + hbit || qhat * dl + hbit > b * rhat + (u(j + n - 2) & MASK) + hbit)) { //Unsigned comparison.
        qhat = qhat - 1
        rhat = rhat + dh
        if (rhat + hbit >= b + hbit) {
          break = true
        }
      }

      // Multiply and subtract.
      k = 0
      var i = 0
      while (i < n) {
        p = qhat * (v(i) & MASK)
        t = (u(i + j) & MASK) - k - (p & MASK)
        u(i + j) = t.toInt
        k = (p >>> 32) - (t >> 32)
        i += 1
      }
      t = (u(j + n) & MASK) - k
      u(j + n) = t.toInt

      q(j) = qhat.toInt // Store quotient digit. If we subtracted too much, add back.
      if (t < 0) {
        q(j) = q(j) - 1
        k = 0
        var i = 0
        while (i < n) {
          t = (u(i + j) & MASK) + (v(i) & MASK) + k
          u(i + j) = t.toInt
          k = t >>> 32 //>>
          i += 1
        }
        u(j + n) += k.toInt
      }
      j -= 1
    }

    if (s > 0) {
      //Unnormalize v[].
      i = 0
      while (i < n - 1) {
        v(i) = v(i) >>> s | v(i + 1) << 32 - s
        i += 1
      }
      v(n - 1) >>>= s

      //Unnormalize u[].
      i = 0
      while (i < m) {
        u(i) = u(i) >>> s | u(i + 1) << 32 - s
        i += 1
      }
      u(m) >>>= s
    }
  }
  /*** </Big Num> ***/

  /*** <Output> ***/
  /**
   * Converts this number into a string of radix 10.
   *
   * @return		The string representation of this number in decimal.
   * @complexity	O(n^2)
   */
  override def toString(): String = {
    if (isZero()) return "0"

    var top = len * 10 + 1
    val buf = Array.fill[Char](top)('0')
    val cpy = java.util.Arrays.copyOf(mag, len)
    var break = false
    while (!break) {
      var j = top
      var tmp = toStringDiv()
      while (tmp > 0) {
        top -= 1
        buf(top) = (buf(top) + tmp % 10).toChar //TODO: Optimize.
        tmp /= 10
      }

      if (len == 1 && mag(0) == 0) {
        break = true
      } else {
        top = j - 13
      }
    }
    if (sign < 0) {
      top -= 1
      buf(top) = '-'
    }
    System.arraycopy(cpy, 0, mag, 0, cpy.length)
    len = cpy.length
    return new String(buf, top, buf.length - top)
  }

  // Divides the number by 10^13 and returns the remainder.
  // Does not change the sign of the number.
  private def toStringDiv(): Long = {
    val pow5 = 1220703125
    val pow2 = 1 << 13
    var nextq = 0
    var rem = 0L
    var i = len - 1
    while (i > 0) {
      rem = (rem << 32) + (mag(i) & MASK)
      val q = (rem / pow5).toInt
      rem = rem % pow5
      mag(i) = nextq | q >>> 13
      nextq = q << 32 - 13
      i -= 1
    }
    rem = (rem << 32) + (mag(0) & MASK)
    val mod2 = mag(0) & pow2 - 1
    mag(0) = nextq | (rem / pow5 >>> 13).toInt
    rem = rem % pow5
    // Applies the Chinese Remainder Theorem.
    // -67*5^13 + 9983778*2^13 = 1
    val pow10 = pow5 * pow2.toLong
    rem = (rem - pow5 * (mod2 - rem) % pow10 * 67) % pow10
    if (rem < 0) rem += pow10
    if (mag(len - 1) == 0 && len > 1) {
      len -= 1
      if (mag(len - 1) == 0 && len > 1)
        len -= 1
    }
    return rem
  }
  /*** </Output> ***/

  /*** <BitOperations> ***/
  // Negative numbers are imagined in their two's complement form with infinite sign extension.
  // This has no effect on bit shifts, but makes implementaion of other bit operations a bit
  // tricky if one wants them to be as efficient as possible.

  /**
   * Shifts this number left by the given amount (less than 32) starting at the given digit,
   * i.e. the first (<len) digits are left untouched.
   *
   * @param shift	The amount to shift.
   * @param first	The digit to start shifting from.
   * @complexity	O(n)
   */
  private def smallShiftLeft(shift: Int, first: Int) {
    var res = mag
    if ((mag(len - 1) << shift >>> shift) != mag(len - 1)) { //Overflow?
      len += 1
      if (len > mag.length) {
        res = Array.ofDim[Int](len + 1) //realloc(len+1)
      } else {
        mag(len - 1) = 0
      }
    }

    var nxt = if (len > mag.length) 0 else mag(len - 1)
    var i = len - 1
    while (i > first) {
      res(i) = nxt << shift | { nxt = mag(i - 1); nxt } >>> 32 - shift
      i -= 1
    }
    res(first) = nxt << shift
    mag = res
  }

  /**
   * Shifts this number right by the given amount (less than 32).
   *
   * @param shift	The amount to shift.
   * @complexity	O(n)
   */
  private def smallShiftRight(shift: Int) {
    var nxt = mag(0)
    var i = 0
    while (i < len - 1) {
      mag(i) = nxt >>> shift | { nxt = mag(i + 1); nxt } << 32 - shift
      i += 1
    }
    mag(len - 1) >>>= shift
    if (mag(len - 1) == 0 && len > 1) {
      len -= 1
    }
  }

  /**
   * Shifts this number left by 32*shift, i.e. moves each digit shift positions to the left.
   *
   * @param shift	The number of positions to move each digit.
   * @complexity	O(n)
   */
  private def bigShiftLeft(shift: Int) {
    if (len + shift > mag.length) {
      val res = Array.ofDim[Int](len + shift + 1)
      System.arraycopy(mag, 0, res, shift, len)
      mag = res
    } else {
      System.arraycopy(mag, 0, mag, shift, len)
      var i = 0
      while (i < shift) {
        mag(i) = 0
        i += 1
      }
    }
    len += shift
  }

  /**
   * Shifts this number right by 32*shift, i.e. moves each digit shift positions to the right.
   *
   * @param shift	The number of positions to move each digit.
   * @complexity	O(n)
   */
  private def bigShiftRight(shift: Int) {
    System.arraycopy(mag, shift, mag, 0, len - shift)
    //for(int i = len-shift; i<len; i++) dig[i] = 0;  dig[j >= len] are allowed to be anything.
    len -= shift
  }

  /**
   * Shifts this number left by the given amount.
   *
   * @param shift	The amount to shift.
   * @complexity	O(n)
   */
  def shiftLeft(shift: Int) {
    val bigShift = shift >>> 5
    val smallShift = shift & 31
    if (bigShift > 0) {
      bigShiftLeft(bigShift)
    }
    if (smallShift > 0) {
      smallShiftLeft(smallShift, bigShift)
    }
  }

  /**
   * Shifts this number right by the given amount.
   *
   * @param shift	The amount to shift.
   * @complexity	O(n)
   */
  def shiftRight(shift: Int) {
    val bigShift = shift >>> 5
    val smallShift = shift & 31
    if (bigShift > 0) {
      bigShiftRight(bigShift)
    }
    if (smallShift > 0) {
      smallShiftRight(smallShift)
    }
  }

  /**
   * Tests if the given bit in the number is set.
   *
   * @param bit	The index of the bit to test.
   * @return true if the given bit is one.
   * @complexity	O(n)
   */
  def testBit(bit: Int): Boolean = {
    val bigBit = bit >>> 5
    val smallBit = bit & 31
    if (bigBit >= len) return sign < 0
    if (sign > 0) return (mag(bigBit) & 1 << smallBit) != 0
    var j = 0
    while (j <= bigBit && mag(j) == 0) {
      j += 1
    }
    if (j > bigBit) return false
    if (j < bigBit) return (mag(bigBit) & 1 << smallBit) == 0
    j = -mag(bigBit)
    return (j & 1 << smallBit) != 0
  }

  /**
   * Sets the given bit in the number.
   *
   * @param bit	The index of the bit to set.
   * @complexity	O(n)
   */
  def setBit(bit: Int) {
    val bigBit = bit >>> 5
    val smallBit = bit & 31
    if (sign > 0) {
      if (bigBit >= mag.length) {
        realloc(bigBit + 1)
        len = bigBit + 1
      } else if (bigBit >= len) {
        while (len <= bigBit) {
          mag(len) = 0
          len += 1
        }
        // len = bigBit+1
      }
      mag(bigBit) |= 1 << smallBit
    } else {
      if (bigBit >= len) return
      var j = 0
      while (j <= bigBit && mag(j) == 0) {
        j += 1
      }
      if (j > bigBit) {
        mag(bigBit) = -1 << smallBit
        while (mag(j) == 0) {
          mag(j) = -1
          j += 1
        }
        mag(j) = ~(-mag(j))
        if (j == len - 1 && mag(len - 1) == 0) len -= 1
        return
      }
      if (j < bigBit) {
        mag(bigBit) &= ~(1 << smallBit)
        while (mag(len - 1) == 0) len -= 1
        return
      }
      j = Integer.lowestOneBit(mag(j)) // more efficient than numberOfTrailingZeros
      val k = 1 << smallBit
      if (k - j > 0) mag(bigBit) &= ~k // Unsigned compare.
      else {
        mag(bigBit) ^= ((j << 1) - 1) ^ (k - 1)
        mag(bigBit) |= k
      }
    }
  }

  /**
   * Clears the given bit in the number.
   *
   * @param bit	The index of the bit to clear.
   * @complexity	O(n)
   */
  def clearBit(bit: Int) {
    val bigBit = bit >>> 5
    val smallBit = bit & 31
    if (sign > 0) {
      if (bigBit < len) {
        mag(bigBit) &= ~(1 << smallBit)
        while (mag(len - 1) == 0 && len > 1) len -= 1
      }
    } else {
      if (bigBit >= mag.length) {
        realloc(bigBit + 1)
        len = bigBit + 1
        mag(bigBit) |= 1 << smallBit
        return
      } else if (bigBit >= len) {
        while (len <= bigBit) {
          mag(len) = 0
          len += 1
        }
        mag(bigBit) |= 1 << smallBit
        return
      }
      var j = 0
      while (j <= bigBit && mag(j) == 0) {
        j += 1
      }
      if (j > bigBit) return
      if (j < bigBit) {
        mag(bigBit) |= 1 << smallBit
        return
      }
      j = java.lang.Integer.lowestOneBit(mag(j)) // more efficient than numberOfTrailingZeros
      var k = 1 << smallBit
      if (j - k > 0) return // Unsigned compare
      if (j - k < 0) {
        mag(bigBit) |= k
        return
      }
      j = mag(bigBit)
      if (j == (-1 ^ k - 1)) {
        mag(bigBit) = 0
        j = bigBit + 1
        while (j < len && mag(j) == -1) {
          mag(j) = 0
          j += 1
        }
        if (j == mag.length) realloc(j + 2)

        if (j == len) {
          len += 1
          mag(len) = 1
          return
        }
        mag(j) = -(~mag(j))
      } else {
        j = java.lang.Integer.lowestOneBit(j ^ (-1 ^ k - 1))
        mag(bigBit) ^= j | (j - 1) ^ (k - 1)
      }
    }
  }

  /**
   * Flips the given bit in the number.
   *
   * @param bit	The index of the bit to flip.
   * @complexity	O(n)
   */
  def flipBit(bit: Int) {
    val bigBit = bit >>> 5
    val smallBit = bit & 31
    breakable {
      if (bigBit >= mag.length) {
        realloc(bigBit + 1)
        len = bigBit + 1
        mag(bigBit) ^= 1 << smallBit
      } else if (bigBit >= len) {
        while (len <= bigBit) {
          mag(len) = 0
          len += 1
        }
        mag(bigBit) ^= 1 << smallBit
      } else if (sign > 0) {
        mag(bigBit) ^= 1 << smallBit
      } else {
        var j = 0
        while (j <= bigBit && mag(j) == 0) {
          j += 1
        }
        if (j < bigBit) {
          mag(bigBit) ^= 1 << smallBit
          break
        }
        if (j > bigBit) { // TODO: Refactor with setBit?
          mag(bigBit) = -1 << smallBit
          while (mag(j) == 0) {
            mag(j) = -1
            j += 1
          }
          mag(j) = ~(-mag(j))
          if (j == len - 1 && mag(len - 1) == 0) {
            len -= 1
          }
          return
        }
        j = java.lang.Integer.lowestOneBit(mag(j)) // more efficient than numberOfTrailingZeros
        val k = 1 << smallBit
        if (j - k > 0) {
          mag(bigBit) ^= ((j << 1) - 1) ^ (k - 1)
          return
        }
        if (j - k < 0) {
          mag(bigBit) ^= k
          return
        }
        j = mag(bigBit)
        if (j == (-1 ^ k - 1)) { // TODO: Refactor with clearBit?
          mag(bigBit) = 0
          j = bigBit + 1
          while (j < len && mag(j) == -1) {
            mag(j) = 0
            j += 1
          }
          if (j == mag.length) realloc(j + 2)
          if (j == len) {
            mag(len) = 1
            len += 1
            return
          }
          mag(j) = -(~mag(j))
        } else {
          j = java.lang.Integer.lowestOneBit(j ^ (-1 ^ k - 1))
          mag(bigBit) ^= j | (j - 1) ^ (k - 1)
        }
      }
    } // end of breakable

    while (mag(len - 1) == 0 && len > 1) len -= 1
  }

  /**
   * Bitwise-ands this number with the given number, i.e. this &= mask.
   *
   * @param mask	The number to bitwise-and with.
   * @complexity	O(n)
   */
  def and(mask: BigInt) {
    if (sign > 0) {
      if (mask.sign > 0) {
        if (mask.len < len) {
          len = mask.len
        }
        var i = 0
        while (i < len) {
          mag(i) &= mask.mag(i)
          i += 1
        }
      } else {
        val mlen = math.min(len, mask.len)
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0 && j < mlen) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = 0
          while (j < mlen && mask.mag(j) == 0) {
            mag(j) = 0
            j += 1
          }
          if (j < mlen) {
            mag(j) &= -mask.mag(j)
          } else if (j == len) {
            len = 1
          }
          j += 1
        } else if (a == 0) { // && (b!=0 || j==mlen)
          while (j < mlen && mag(j) == 0) {
            j += 1
          }
        } else {
          mag(j - 1) &= -b
        }
        while (j < mlen) {
          mag(j) &= ~mask.mag(j)
          j += 1
        }
      }
      while (mag(len - 1) == 0 && len > 1) {
        len -= 1
      }
    } else {
      val mlen = math.min(len, mask.len)
      if (mask.sign > 0) {
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0 && j < mlen) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = 0
          while (j < mlen && mask.mag(j) == 0) {
            mag(j) = 0
            j += 1
          }
        } else if (a == 0) { // && (b!=0 || j==mlen)
          while (j < mlen && mag(j) == 0) {
            j += 1
          }
          if (j < mlen) {
            mag(j) = -mag(j) & mask.mag(j)
          }
          j += 1
        } else {
          mag(j - 1) = -a & b
        }
        while (j < mlen) {
          mag(j) = ~mag(j) & mask.mag(j)
          j += 1
        }
        if (mask.len > len) {
          if (mask.len > mag.length) {
            realloc(mask.len + 2)
          }
          System.arraycopy(mask.mag, len, mag, len, mask.len - len)
        }
        len = mask.len
        sign = 1
        while (mag(len - 1) == 0 && len > 1) {
          len -= 1
        }
      } else {
        if (mask.len > len) {
          if (mask.len > mag.length) {
            realloc(mask.len + 2)
          }
          System.arraycopy(mask.mag, len, mag, len, mask.len - len)
        }
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = 0
          while (j < mlen && mask.mag(j) == 0) {
            mag(j) = 0
            j += 1
          }
          if (j < mlen) {
            mag(j) = -(~mag(j) & -mask.mag(j))
          }
          j += 1
        } else if (a == 0) { // && (b!=0 || j==mlen)
          while (j < mlen && mag(j) == 0) {
            j += 1
          }
          if (j < mlen) {
            mag(j) = -(-mag(j) & ~mask.mag(j))
          }
          j += 1
        } else {
          mag(j - 1) = -(-a & -b)
        }
        if (j <= mlen && mag(j - 1) == 0) {
          if (j < mlen) {
            mag(j) = -(~(mag(j) | mask.mag(j)))
            while ({ j += 1; j < mlen } && mag(j - 1) == 0) { //// HERE
              mag(j) = -(~(mag(j) | mask.mag(j))) // -(~dig[j]&~mask.dig[j])
            }
          }
          if (j == mlen && mag(j - 1) == 0) {
            val blen = math.max(len, mask.len)
            while (j < blen && mag(j) == -1) {
              mag(j) = 0 // mask.dig[j]==dig[j]
              j += 1
            }
            if (j < blen) {
              mag(j) = -(~mag(j))
            } else {
              if (blen >= mag.length) {
                realloc(blen + 2)
              }
              mag(blen) = 1
              len = blen + 1
              return
            }
            j += 1
          }
        }
        while (j < mlen) {
          mag(j) |= mask.mag(j) // ~(~dig[j]&~mask.dig[j])
          j += 1
        }
        if (mask.len > len) {
          len = mask.len
        }
      }
    }
  }

  /**
   * Bitwise-ors this number with the given number, i.e. this |= mask.
   *
   * @param mask	The number to bitwise-or with.
   * @complexity	O(n)
   */
  def or(mask: BigInt) {
    if (sign > 0) {
      if (mask.sign > 0) {
        if (mask.len > len) {
          if (mask.len > mag.length) realloc(mask.len + 1)
          System.arraycopy(mask.mag, len, mag, len, mask.len - len)
          var i = 0
          while (i < len) {
            mag(i) |= mask.mag(i)
            i += 1
          }
          len = mask.len
        } else {
          var i = 0
          while (i < mask.len) {
            mag(i) |= mask.mag(i)
            i += 1
          }
        }
      } else {
        if (mask.len > mag.length) realloc(mask.len + 1)
        if (mask.len > len) { System.arraycopy(mask.mag, len, mag, len, mask.len - len) }
        val mLen = math.min(mask.len, len)
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0 && j < mLen) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = -a
          while (mask.mag(j) == 0) {
            mag(j) ^= -1
            j += 1
          }
          if (j < mLen) {
            mag(j) = ~(mag(j) | -mask.mag(j))
          } else {
            // mask.dig[j] == dig[j]
            mag(j) = ~(-mag(j))
          }
          j += 1
        } else if (a == 0) { // && (b!=0 || j==mLen)
          mag(j - 1) = b
          while (j < mLen && mag(j) == 0) {
            mag(j) = mask.mag(j)
            j += 1
          }
        } else { // a!=0 && b!=0
          mag(j - 1) = -(a | -b)
        }
        while (j < mLen) {
          mag(j) = ~mag(j) & mask.mag(j) // ~(dig[j]|~mask.dig[j])
          j += 1
        }
        sign = -1
        len = mask.len
        while (mag(len - 1) == 0) {
          len -= 1
        }
      }
    } else {
      val mLen = math.min(mask.len, len)
      var a = mag(0)
      var b = mask.mag(0)
      var j = 1
      while ((a | b) == 0 && j < mLen) {
        a = mag(j)
        b = mask.mag(j)
        j += 1
      }
      if (mask.sign > 0) {
        if (a != 0 && b == 0) {
          while (j < mLen && mask.mag(j) == 0) {
            j += 1
          }
        } else if (a == 0) { // && (b!=0 || j==mLen)
          mag(j - 1) = -b
          while (j < mLen && mag(j) == 0) {
            mag(j) = ~mask.mag(j)
            j += 1
          }
          if (j < mLen) {
            mag(j) = ~(-mag(j) | mask.mag(j))
          } else {
            while (mag(j) == 0) {
              mag(j) = -1
              j += 1
            }
            mag(j) = ~(-mag(j))
          }
          j += 1
        } else { // a!=0 && b!=0
          mag(j - 1) = -(-a | b)
        }
        while (j < mLen) {
          mag(j) &= ~mask.mag(j) // ~(~dig[j]|mask.dig[j])
          j += 1
        }
      } else {
        if (a != 0 && b == 0) {
          while (j < mLen && mask.mag(j) == 0) {
            j += 1
          }
          if (j < mLen) {
            mag(j) = ~(~mag(j) | -mask.mag(j))
          }
          j += 1
        } else if (a == 0) { // && (b!=0 || j==mLen)
          mag(j - 1) = b
          while (j < mLen && mag(j) == 0) {
            mag(j) = mask.mag(j)
            j += 1
          }
          if (j < mLen) {
            mag(j) = ~(-mag(j) | ~mask.mag(j))
          }
          j += 1
        } else { // a!=0 && b!=0
          mag(j - 1) = -(-a | -b)
        }
        while (j < mLen) {
          mag(j) &= mask.mag(j) // ~(~dig[j]|~mask.dig[j])
          j += 1
        }
        len = mLen
      }
      while (mag(len - 1) == 0) {
        len -= 1
      }
    }
  }

  /**
   * Bitwise-xors this number with the given number, i.e. this ^= mask.
   *
   * @param mask	The number to bitwise-xor with.
   * @complexity	O(n)
   */
  def xor(mask: BigInt) {
    if (sign > 0) {
      if (mask.len > len) {
        if (mask.len > mag.length) realloc(mask.len + 2)
        System.arraycopy(mask.mag, len, mag, len, mask.len - len)
      }
      val mlen = math.min(len, mask.len)
      if (mask.sign > 0) {
        var i = 0
        while (i < mlen) {
          mag(i) ^= mask.mag(i)
          i += 1
        }
      } else {
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0 && j < mlen) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = -a
          while (mask.mag(j) == 0) {
            mag(j) ^= -1
            j += 1
          }
          if (j < len) {
            mag(j) = ~(mag(j) ^ -mask.mag(j))
          } else {
            mag(j) = ~(-mask.mag(j))
          }
          j += 1
        } else if (a == 0) { // && (b!=0 || j==mLen)
          mag(j - 1) = b // -(0^-b)
        } else { // a!=0 && b!=0
          mag(j - 1) = -(a ^ -b)
          while (j < mlen && mag(j - 1) == 0) {
            mag(j) = -(mag(j) ^ ~mask.mag(j))
            j += 1
          }
          if (j >= mlen && mag(j - 1) == 0) {
            val tmp = if (j < len) mag else mask.mag
            val blen = math.max(len, mask.len)
            while (j < blen && tmp(j) == -1) {
              mag(j) = 0
              j += 1
            }
            if (blen == mag.length) realloc(blen + 2) // len==blen
            if (j == blen) {
              mag(blen) = 1
              len = blen + 1
            } else {
              mag(j) = -(~tmp(j))
            }
            j += 1
          }
        }
        while (j < mlen) {
          mag(j) ^= mask.mag(j) // ~(dig[j]^~mask.dig[j])
          j += 1
        }
        sign = -1
      }
      if (mask.len > len) {
        len = mask.len
      } else {
        while (mag(len - 1) == 0 && len > 1) len -= 1
      }
    } else {
      if (mask.len > len) {
        if (mask.len > mag.length) realloc(mask.len + 2)
        System.arraycopy(mask.mag, len, mag, len, mask.len - len)
      }
      val mlen = math.min(len, mask.len)
      if (mask.sign > 0) {
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0 && j < mlen) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          while (j < mlen && mask.mag(j) == 0) j += 1
        } else if (a == 0) { // && (b!=0 || j==mLen)
          mag(j - 1) = -b
          while (j < mlen && mag(j) == 0) {
            mag(j) = ~mask.mag(j)
            j += 1
          }
          while (j < len && mag(j) == 0) {
            mag(j) = -1
            j += 1
          }
          if (j < mlen) {
            mag(j) = ~(-mag(j) ^ mask.mag(j))
          } else {
            mag(j) = ~(-mag(j))
          }
          j += 1
        } else { // a!=0 && b!=0
          mag(j - 1) = -(-a ^ b)
        }
        while (j < mlen) {
          mag(j) ^= mask.mag(j) // ~(~dig[j]^mask.dig[j])
          j += 1
        }
      } else {
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while ((a | b) == 0 && j < mlen) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = -a
          while (mask.mag(j) == 0) {
            mag(j) ^= -1 // ~dig[j]
            j += 1
          }
          if (j < len) {
            mag(j) = ~mag(j) ^ -mask.mag(j)
          } else {
            mag(j) = ~(-mag(j)) // dig[j]==mask.dig[j], ~0^-mask.dig[j]
          }
          j += 1
        } else if (a == 0) { // && b!=0
          mag(j - 1) = -b
          while (j < mask.len && mag(j) == 0) {
            mag(j) = ~mask.mag(j)
            j += 1
          }
          while (mag(j) == 0) {
            mag(j) = -1
            j += 1
          }
          if (j < mask.len) {
            mag(j) = -mag(j) ^ ~mask.mag(j)
          } else {
            mag(j) = ~(-mag(j)) // -dig[j]^~0
          }
          j += 1
        } else { // a!=0 && b!=0
          mag(j - 1) = -a ^ -b
        }
        while (j < mlen) {
          mag(j) ^= mask.mag(j) // ~dig[j]^~mask.dig[j]
          j += 1
        }
        sign = 1
      }
      if (mask.len > len) {
        len = mask.len
      } else {
        while (mag(len - 1) == 0 && len > 1) len -= 1
      }
    }
  }

  /**
   * Bitwise-and-nots this number with the given number, i.e. this &= ~mask.
   *
   * @param mask	The number to bitwise-and-not with.
   * @complexity	O(n)
   */
  def andNot(mask: BigInt) {
    val mlen = math.min(len, mask.len)
    if (sign > 0) {
      if (mask.sign > 0) {
        var i = 0
        while (i < mlen) {
          mag(i) &= ~mask.mag(i)
          i += 1
        }
      } else {
        var j = 0
        while (j < mlen && mask.mag(j) == 0) {
          j += 1
        }
        if (j < mlen) {
          mag(j) &= ~(-mask.mag(j))
          while ({ j += 1; j < mlen }) {
            mag(j) &= mask.mag(j) // ~~mask.dig[j]
          }
        }
        len = mlen
      }
    } else {
      if (mask.len > len) {
        if (mask.len > mag.length) realloc(mask.len + 2)
        System.arraycopy(mask.mag, len, mag, len, mask.len - len)
      }
      if (mask.sign > 0) {
        var j = 0
        while (mag(j) == 0) {
          j += 1
        }
        if (j < mlen) {
          mag(j) = -(-mag(j) & ~mask.mag(j))
          while ({ j += 1; j < mlen } && mag(j - 1) == 0) {
            mag(j) = -(~(mag(j) | mask.mag(j))) // -(~dig[j]&~mask.dig[j])
          }
          if (j == mlen && mag(j - 1) == 0) {
            val blen = math.max(len, mask.len)
            while (j < blen && mag(j) == -1) {
              mag(j) = 0 // mask.dig[j]==dig[j]
              j += 1
            }
            if (j < blen) {
              mag(j) = -(~mag(j))
            } else {
              if (blen >= mag.length) realloc(blen + 2)
              mag(blen) = 1
              len = blen + 1
              return
            }
            j += 1
          }
          while (j < mlen) {
            mag(j) |= mask.mag(j) // ~(~dig[j]&~mask.dig[j])
            j += 1
          }
          if (mask.len > len) {
            len = mask.len
          }
        }
      } else {
        var a = mag(0)
        var b = mask.mag(0)
        var j = 1
        while (j < mlen && (a | b) == 0) {
          a = mag(j)
          b = mask.mag(j)
          j += 1
        }
        if (a != 0 && b == 0) {
          mag(j - 1) = -a
          while (j < mask.len && mask.mag(j) == 0) {
            mag(j) ^= -1
            j += 1
          }
          if (j < len) {
            mag(j) = ~(mag(j) | -mask.mag(j)) // ~dig[j]&~-mask.dig[j])
          } else {
            mag(j) = ~(-mag(j)) // dig[j]==mask.dig[j]
          }
          j += 1
        } else if (a == 0) { // && (b!=0 || j==mlen)
          while (j < mlen && mag(j) == 0) {
            j += 1
          }
          if (j < mlen) {
            mag(j) = -mag(j) & mask.mag(j) // ~~mask.dig[j]
          }
          j += 1
        } else {
          mag(j - 1) = -a & ~(-b)
        }
        while (j < mlen) {
          mag(j) = ~mag(j) & mask.mag(j)
          j += 1
        }
        len = mask.len
        sign = 1
      }
    }
    while (mag(len - 1) == 0 && len > 1) len -= 1
  }

  /**
   * Inverts sign and all bits of this number, i.e. this = ~this.
   * The identity -this = ~this + 1 holds.
   *
   * @complexity	O(n)
   */
  def not() {
    if (sign > 0) {
      sign = -1
      uaddMag(1)
    } else {
      sign = 1
      usubMag(1)
    }
  }
  /*** </BitOperations> ***/
}