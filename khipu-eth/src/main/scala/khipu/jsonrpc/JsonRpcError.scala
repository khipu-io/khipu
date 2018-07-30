package khipu.jsonrpc

import org.json4s.JsonAST.JValue

final case class JsonRpcError(code: Int, message: String, data: Option[JValue])

// scalastyle:off magic.number
// scalastyle:off public.methods.have.type
object JsonRpcErrors {
  val ParseError = JsonRpcError(-32700, "An error occurred on the server while parsing the JSON text", None)
  val InvalidRequest = JsonRpcError(-32600, "The JSON sent is not a valid Request object", None)
  val MethodNotFound = JsonRpcError(-32601, "The method does not exist / is not available", None)
  def InvalidParams(msg: String = "Invalid method parameters") = JsonRpcError(-32602, msg, None)
  val InternalError = JsonRpcError(-32603, "Internal JSON-RPC error", None)
  def LogicError(msg: String) = JsonRpcError(-32000, msg, None)
  val AccountLocked = LogicError("account is locked or unknown")
}
