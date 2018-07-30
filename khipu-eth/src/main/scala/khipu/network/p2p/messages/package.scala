package khipu.network.p2p

import khipu.network.p2p.Message.Version

package object messages {
  object Versions {
    val PV61: Version = 61
    val PV62: Version = 62
    val PV63: Version = 63

    val SubProtocolOffset = 0x10
  }
}
