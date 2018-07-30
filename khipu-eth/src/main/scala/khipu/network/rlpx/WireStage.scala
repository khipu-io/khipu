package khipu.network.rlpx

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import khipu.network.Control
import khipu.network.WireDisconnected

final class WireStage extends GraphStage[FlowShape[ByteString, Either[Control, ByteString]]] {

  val in = Inlet[ByteString]("WireStage.in")
  val out = Outlet[Either[Control, ByteString]]("WireStage.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush() {
        push(out, Right(grab(in)))
      }

      override def onUpstreamFinish() {
        emitDisconnectedSignal()
        super.onUpstreamFinish()
      }
    })

    private def emitDisconnectedSignal() {
      emit(out, Left(WireDisconnected))
    }
  }
}