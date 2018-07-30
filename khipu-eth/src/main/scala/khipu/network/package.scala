package khipu

package object network {
  sealed trait Control
  case object Tick extends Control
  case object WireDisconnected extends Control
}

