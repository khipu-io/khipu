package khipu.jsonrpc

import org.json4s.JsonAST.{ JArray, JValue }

final case class JsonRpcRequest(jsonrpc: String, method: String, params: Option[JArray], id: Option[JValue])
