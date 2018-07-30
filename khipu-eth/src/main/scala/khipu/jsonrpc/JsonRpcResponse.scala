package khipu.jsonrpc

import org.json4s.JsonAST.JValue

final case class JsonRpcResponse(jsonrpc: String, result: Option[JValue], error: Option[JsonRpcError], id: JValue)
