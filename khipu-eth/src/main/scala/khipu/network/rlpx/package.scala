package khipu.network

import org.spongycastle.crypto.digests.KeccakDigest

package object rlpx {
  final case class Secrets(
    aes:        Array[Byte],
    mac:        Array[Byte],
    token:      Array[Byte],
    egressMac:  KeccakDigest,
    ingressMac: KeccakDigest
  )
}
