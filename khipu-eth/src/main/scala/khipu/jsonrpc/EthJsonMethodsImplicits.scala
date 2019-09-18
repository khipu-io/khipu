package khipu.jsonrpc

import akka.util.ByteString
import java.math.BigInteger
import khipu.Hash
import khipu.DataWord
import khipu.jsonrpc.EthService.BestBlockNumberRequest
import khipu.jsonrpc.EthService.BestBlockNumberResponse
import khipu.jsonrpc.EthService.BlockByBlockHashRequest
import khipu.jsonrpc.EthService.BlockByBlockHashResponse
import khipu.jsonrpc.EthService.BlockByNumberRequest
import khipu.jsonrpc.EthService.BlockByNumberResponse
import khipu.jsonrpc.EthService.BlockParam
import khipu.jsonrpc.EthService.CallRequest
import khipu.jsonrpc.EthService.CallResponse
import khipu.jsonrpc.EthService.CallTx
import khipu.jsonrpc.EthService.EstimateGasResponse
import khipu.jsonrpc.EthService.Filter
import khipu.jsonrpc.EthService.GetBalanceRequest
import khipu.jsonrpc.EthService.GetBalanceResponse
import khipu.jsonrpc.EthService.GetBlockTransactionCountByNumberRequest
import khipu.jsonrpc.EthService.GetBlockTransactionCountByNumberResponse
import khipu.jsonrpc.EthService.GetCodeRequest
import khipu.jsonrpc.EthService.GetCodeResponse
import khipu.jsonrpc.EthService.GetCoinbaseRequest
import khipu.jsonrpc.EthService.GetCoinbaseResponse
import khipu.jsonrpc.EthService.GetFilterChangesRequest
import khipu.jsonrpc.EthService.GetFilterChangesResponse
import khipu.jsonrpc.EthService.GetFilterLogsRequest
import khipu.jsonrpc.EthService.GetFilterLogsResponse
import khipu.jsonrpc.EthService.GetGasPriceRequest
import khipu.jsonrpc.EthService.GetGasPriceResponse
import khipu.jsonrpc.EthService.GetHashRateRequest
import khipu.jsonrpc.EthService.GetHashRateResponse
import khipu.jsonrpc.EthService.GetLogsRequest
import khipu.jsonrpc.EthService.GetLogsResponse
import khipu.jsonrpc.EthService.GetMiningRequest
import khipu.jsonrpc.EthService.GetMiningResponse
import khipu.jsonrpc.EthService.GetStorageAtRequest
import khipu.jsonrpc.EthService.GetStorageAtResponse
import khipu.jsonrpc.EthService.GetTransactionByBlockHashAndIndexRequest
import khipu.jsonrpc.EthService.GetTransactionByBlockHashAndIndexResponse
import khipu.jsonrpc.EthService.GetTransactionByBlockNumberAndIndexRequest
import khipu.jsonrpc.EthService.GetTransactionByBlockNumberAndIndexResponse
import khipu.jsonrpc.EthService.GetTransactionByHashRequest
import khipu.jsonrpc.EthService.GetTransactionByHashResponse
import khipu.jsonrpc.EthService.GetTransactionCountRequest
import khipu.jsonrpc.EthService.GetTransactionCountResponse
import khipu.jsonrpc.EthService.GetTransactionReceiptRequest
import khipu.jsonrpc.EthService.GetTransactionReceiptResponse
import khipu.jsonrpc.EthService.GetUncleCountByBlockHashRequest
import khipu.jsonrpc.EthService.GetUncleCountByBlockHashResponse
import khipu.jsonrpc.EthService.GetUncleCountByBlockNumberRequest
import khipu.jsonrpc.EthService.GetUncleCountByBlockNumberResponse
import khipu.jsonrpc.EthService.GetWorkRequest
import khipu.jsonrpc.EthService.GetWorkResponse
import khipu.jsonrpc.EthService.NewBlockFilterRequest
import khipu.jsonrpc.EthService.NewFilterRequest
import khipu.jsonrpc.EthService.NewFilterResponse
import khipu.jsonrpc.EthService.NewPendingTransactionFilterRequest
import khipu.jsonrpc.EthService.ProtocolVersionRequest
import khipu.jsonrpc.EthService.ProtocolVersionResponse
import khipu.jsonrpc.EthService.SendRawTransactionRequest
import khipu.jsonrpc.EthService.SendRawTransactionResponse
import khipu.jsonrpc.EthService.SubmitHashRateRequest
import khipu.jsonrpc.EthService.SubmitHashRateResponse
import khipu.jsonrpc.EthService.SubmitWorkRequest
import khipu.jsonrpc.EthService.SubmitWorkResponse
import khipu.jsonrpc.EthService.SyncingRequest
import khipu.jsonrpc.EthService.SyncingResponse
import khipu.jsonrpc.EthService.TxCountByBlockHashRequest
import khipu.jsonrpc.EthService.TxCountByBlockHashResponse
import khipu.jsonrpc.EthService.UncleByBlockHashAndIndexRequest
import khipu.jsonrpc.EthService.UncleByBlockHashAndIndexResponse
import khipu.jsonrpc.EthService.UncleByBlockNumberAndIndexRequest
import khipu.jsonrpc.EthService.UncleByBlockNumberAndIndexResponse
import khipu.jsonrpc.EthService.UninstallFilterRequest
import khipu.jsonrpc.EthService.UninstallFilterResponse
import khipu.jsonrpc.JsonRpcController.{ JsonDecoder, JsonEncoder }
import khipu.jsonrpc.JsonRpcErrors.InvalidParams
import khipu.jsonrpc.PersonalService.{ SendTransactionRequest, SendTransactionResponse, SignRequest }
import org.json4s.{ Extraction, JsonAST }
import org.json4s.JsonAST.{ JArray, JBool, JString, JValue, _ }
import org.json4s.JsonDSL._

// scalastyle:off number.of.methods
object EthJsonMethodsImplicits extends JsonMethodsImplicits {

  implicit val eth_protocolVersion = new JsonDecoder[ProtocolVersionRequest] with JsonEncoder[ProtocolVersionResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, ProtocolVersionRequest] = Right(ProtocolVersionRequest())

    def encodeJson(t: ProtocolVersionResponse): JValue = t.value
  }

  implicit val eth_blockNumber = new JsonDecoder[BestBlockNumberRequest] with JsonEncoder[BestBlockNumberResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, BestBlockNumberRequest] = Right(BestBlockNumberRequest())

    override def encodeJson(t: BestBlockNumberResponse): JValue = Extraction.decompose(t.bestBlockNumber)
  }

  implicit val eth_submitHashrate = new JsonDecoder[SubmitHashRateRequest] with JsonEncoder[SubmitHashRateResponse] {
    override def decodeJson(params: Option[JsonAST.JArray]): Either[JsonRpcError, SubmitHashRateRequest] = params match {
      case Some(JArray(hashRate :: JString(id) :: Nil)) =>
        val result: Either[JsonRpcError, SubmitHashRateRequest] = for {
          rate <- extractQuantity(hashRate)
          miner <- extractBytes(id)
        } yield SubmitHashRateRequest(rate, miner)
        result
      case _ =>
        Left(InvalidParams())
    }

    override def encodeJson(t: SubmitHashRateResponse): JValue = JBool(t.success)
  }

  implicit val eth_gasPrice = new JsonDecoder[GetGasPriceRequest] with JsonEncoder[GetGasPriceResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetGasPriceRequest] = params match {
      case None | Some(JArray(Nil)) => Right(GetGasPriceRequest())
      case Some(_)                  => Left(InvalidParams())
    }

    override def encodeJson(t: GetGasPriceResponse): JValue = encodeAsHex(t.price)
  }

  implicit val eth_mining = new JsonDecoder[GetMiningRequest] with JsonEncoder[GetMiningResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetMiningRequest] = params match {
      case None | Some(JArray(Nil)) => Right(GetMiningRequest())
      case Some(_)                  => Left(InvalidParams())
    }

    override def encodeJson(t: GetMiningResponse): JValue = JBool(t.isMining)
  }

  implicit val eth_hashrate = new JsonDecoder[GetHashRateRequest] with JsonEncoder[GetHashRateResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetHashRateRequest] = params match {
      case None | Some(JArray(Nil)) => Right(GetHashRateRequest())
      case Some(_)                  => Left(InvalidParams())
    }

    override def encodeJson(t: GetHashRateResponse): JsonAST.JValue = encodeAsHex(t.hashRate)
  }

  implicit val eth_coinbase = new JsonDecoder[GetCoinbaseRequest] with JsonEncoder[GetCoinbaseResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetCoinbaseRequest] = params match {
      case None | Some(JArray(Nil)) => Right(GetCoinbaseRequest())
      case Some(_)                  => Left(InvalidParams())
    }

    override def encodeJson(t: GetCoinbaseResponse): JsonAST.JValue = {
      encodeAsHex(t.address.bytes)
    }
  }

  implicit val eth_getWork = new JsonDecoder[GetWorkRequest] with JsonEncoder[GetWorkResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetWorkRequest] = params match {
      case None | Some(JArray(Nil)) => Right(GetWorkRequest())
      case Some(_)                  => Left(InvalidParams())
    }

    override def encodeJson(t: GetWorkResponse): JsonAST.JValue = {
      val powHeaderHash = encodeAsHex(t.powHeaderHash)
      val dagSeed = encodeAsHex(t.dagSeed)
      val target = encodeAsHex(t.target)
      JArray(List(powHeaderHash, dagSeed, target))
    }
  }

  implicit val eth_submitWork = new JsonDecoder[SubmitWorkRequest] with JsonEncoder[SubmitWorkResponse] {
    override def decodeJson(params: Option[JsonAST.JArray]): Either[JsonRpcError, SubmitWorkRequest] = params match {
      case Some(JArray(JString(nonce) :: JString(powHeaderHash) :: JString(mixHash) :: Nil)) =>
        for {
          n <- extractBytes(nonce)
          p <- extractHash(powHeaderHash)
          m <- extractHash(mixHash)
        } yield SubmitWorkRequest(n, p, m)
      case _ =>
        Left(InvalidParams())
    }

    override def encodeJson(t: SubmitWorkResponse): JValue = JBool(t.success)
  }

  implicit val eth_getBlockTransactionCountByHash = new JsonDecoder[TxCountByBlockHashRequest] with JsonEncoder[TxCountByBlockHashResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, TxCountByBlockHashRequest] =
      params match {
        case Some(JArray(JString(input) :: Nil)) =>
          extractHash(input).map(TxCountByBlockHashRequest)
        case _ => Left(InvalidParams())
      }

    override def encodeJson(t: TxCountByBlockHashResponse): JValue =
      Extraction.decompose(t.txsQuantity.map(BigInteger.valueOf(_)))
  }

  implicit val eth_getBlockByHash = new JsonDecoder[BlockByBlockHashRequest] with JsonEncoder[BlockByBlockHashResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, BlockByBlockHashRequest] = {
      params match {
        case Some(JArray(JString(blockHash) :: JBool(fullTxs) :: Nil)) =>
          extractHash(blockHash).map(BlockByBlockHashRequest(_, fullTxs))
        case _ => Left(InvalidParams())
      }
    }

    override def encodeJson(t: BlockByBlockHashResponse): JValue =
      Extraction.decompose(t.blockResponse)
  }

  implicit val eth_getBlockByNumber = new JsonDecoder[BlockByNumberRequest] with JsonEncoder[BlockByNumberResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, BlockByNumberRequest] = {
      params match {
        case Some(JArray(blockStr :: JBool(fullTxs) :: Nil)) =>
          extractBlockParam(blockStr).map(BlockByNumberRequest(_, fullTxs))
        case _ => Left(InvalidParams())
      }
    }

    override def encodeJson(t: BlockByNumberResponse): JValue =
      Extraction.decompose(t.blockResponse)
  }

  implicit val eth_getTransactionByHash =
    new JsonDecoder[GetTransactionByHashRequest] with JsonEncoder[GetTransactionByHashResponse] {
      override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetTransactionByHashRequest] = params match {
        case Some(JArray(JString(txHash) :: Nil)) =>
          for {
            parsedTxHash <- extractHash(txHash)
          } yield GetTransactionByHashRequest(parsedTxHash)
        case _ => Left(InvalidParams())
      }

      override def encodeJson(t: GetTransactionByHashResponse): JValue =
        Extraction.decompose(t.txResponse)
    }

  implicit val eth_getTransactionReceipt =
    new JsonDecoder[GetTransactionReceiptRequest] with JsonEncoder[GetTransactionReceiptResponse] {
      override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetTransactionReceiptRequest] = params match {
        case Some(JArray(JString(txHash) :: Nil)) =>
          for {
            parsedTxHash <- extractHash(txHash)
          } yield GetTransactionReceiptRequest(parsedTxHash)
        case _ => Left(InvalidParams())
      }

      override def encodeJson(t: GetTransactionReceiptResponse): JValue =
        Extraction.decompose(t.txResponse)
    }

  implicit val eth_getTransactionByBlockHashAndIndex =
    new JsonDecoder[GetTransactionByBlockHashAndIndexRequest] with JsonEncoder[GetTransactionByBlockHashAndIndexResponse] {
      override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetTransactionByBlockHashAndIndexRequest] = params match {
        case Some(JArray(JString(blockHash) :: transactionIndex :: Nil)) =>
          for {
            parsedBlockHash <- extractHash(blockHash)
            parsedTransactionIndex <- extractQuantity(transactionIndex)
          } yield GetTransactionByBlockHashAndIndexRequest(parsedBlockHash, parsedTransactionIndex.longValue)
        case _ => Left(InvalidParams())
      }

      override def encodeJson(t: GetTransactionByBlockHashAndIndexResponse): JValue =
        t.transactionResponse.map(Extraction.decompose).getOrElse(JNull)
    }

  implicit val eth_getTransactionByBlockNumberAndIndex =
    new JsonDecoder[GetTransactionByBlockNumberAndIndexRequest] with JsonEncoder[GetTransactionByBlockNumberAndIndexResponse] {
      override def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetTransactionByBlockNumberAndIndexRequest] = params match {
        case Some(JArray(blockParam :: transactionIndex :: Nil)) =>
          for {
            blockParam <- extractBlockParam(blockParam)
            parsedTransactionIndex <- extractQuantity(transactionIndex)
          } yield GetTransactionByBlockNumberAndIndexRequest(blockParam, parsedTransactionIndex.longValue)
        case _ => Left(InvalidParams())
      }

      override def encodeJson(t: GetTransactionByBlockNumberAndIndexResponse): JValue =
        t.transactionResponse.map(Extraction.decompose).getOrElse(JNull)
    }

  implicit val eth_getUncleByBlockHashAndIndex = new JsonDecoder[UncleByBlockHashAndIndexRequest] with JsonEncoder[UncleByBlockHashAndIndexResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, UncleByBlockHashAndIndexRequest] =
      params match {
        case Some(JArray(JString(blockHash) :: uncleIndex :: Nil)) =>
          for {
            hash <- extractHash(blockHash)
            uncleBlockIndex <- extractQuantity(uncleIndex)
          } yield UncleByBlockHashAndIndexRequest(hash, uncleBlockIndex.longValue)
        case _ => Left(InvalidParams())
      }

    override def encodeJson(t: UncleByBlockHashAndIndexResponse): JValue = {
      val uncleBlockResponse = Extraction.decompose(t.uncleBlockResponse)
      uncleBlockResponse.removeField {
        case JField("transactions", _) => true
        case _                         => false
      }
    }
  }

  implicit val eth_getUncleByBlockNumberAndIndex = new JsonDecoder[UncleByBlockNumberAndIndexRequest] with JsonEncoder[UncleByBlockNumberAndIndexResponse] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, UncleByBlockNumberAndIndexRequest] =
      params match {
        case Some(JArray(blockStr :: uncleIndex :: Nil)) =>
          for {
            block <- extractBlockParam(blockStr)
            uncleBlockIndex <- extractQuantity(uncleIndex)
          } yield UncleByBlockNumberAndIndexRequest(block, uncleBlockIndex.longValue)
        case _ => Left(InvalidParams())
      }

    override def encodeJson(t: UncleByBlockNumberAndIndexResponse): JValue = {
      val uncleBlockResponse = Extraction.decompose(t.uncleBlockResponse)
      uncleBlockResponse.removeField {
        case JField("transactions", _) => true
        case _                         => false
      }
    }
  }

  implicit val eth_syncing = new JsonDecoder[SyncingRequest] with JsonEncoder[SyncingResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, SyncingRequest] = Right(SyncingRequest())

    def encodeJson(t: SyncingResponse): JValue = t.syncStatus match {
      case Some(syncStatus) => Extraction.decompose(syncStatus)
      case None             => false
    }
  }

  implicit val eth_sendRawTransaction = new JsonDecoder[SendRawTransactionRequest] with JsonEncoder[SendRawTransactionResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, SendRawTransactionRequest] =
      params match {
        case Some(JArray(JString(dataStr) :: Nil)) =>
          for {
            data <- extractBytes(dataStr)
          } yield SendRawTransactionRequest(data)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: SendRawTransactionResponse): JValue = encodeAsHex(t.transactionHash: Hash)
  }

  implicit val eth_sendTransaction = new Codec[SendTransactionRequest, SendTransactionResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, SendTransactionRequest] =
      params match {
        case Some(JArray(JObject(tx) :: _)) =>
          extractTx(tx.toMap).map(SendTransactionRequest)
        case _ =>
          Left(InvalidParams())
      }

    def encodeJson(t: SendTransactionResponse): JValue =
      encodeAsHex(t.txHash)
  }

  implicit val eth_call = new JsonDecoder[CallRequest] with JsonEncoder[CallResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, CallRequest] =
      params match {
        case Some(JArray((txObj: JObject) :: (blockValue: JValue) :: Nil)) =>
          for {
            blockParam <- extractBlockParam(blockValue)
            tx <- extractCall(txObj)
          } yield CallRequest(tx, blockParam)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: CallResponse): JValue = encodeAsHex(t.returnData)
  }

  implicit val eth_estimateGas = new JsonDecoder[CallRequest] with JsonEncoder[EstimateGasResponse] {
    override def encodeJson(t: EstimateGasResponse): JValue = encodeAsHex(t.gas)

    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, CallRequest] =
      withoutBlockParam.applyOrElse(params, eth_call.decodeJson)

    def withoutBlockParam: PartialFunction[Option[JArray], Either[JsonRpcError, CallRequest]] = {
      case Some(JArray((txObj: JObject) :: Nil)) =>
        extractCall(txObj).map(CallRequest(_, BlockParam.Latest))
    }

  }

  implicit val eth_getCode = new JsonDecoder[GetCodeRequest] with JsonEncoder[GetCodeResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetCodeRequest] =
      params match {
        case Some(JArray((address: JString) :: (blockValue: JValue) :: Nil)) =>
          for {
            addr <- extractAddress(address)
            block <- extractBlockParam(blockValue)
          } yield GetCodeRequest(addr, block)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetCodeResponse): JValue = encodeAsHex(t.result)
  }

  implicit val eth_getUncleCountByBlockNumber = new JsonDecoder[GetUncleCountByBlockNumberRequest] with JsonEncoder[GetUncleCountByBlockNumberResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetUncleCountByBlockNumberRequest] =
      params match {
        case Some(JArray((blockValue: JValue) :: Nil)) =>
          for {
            block <- extractBlockParam(blockValue)
          } yield GetUncleCountByBlockNumberRequest(block)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetUncleCountByBlockNumberResponse): JValue = encodeAsHex(t.result)
  }

  implicit val eth_getUncleCountByBlockHash = new JsonDecoder[GetUncleCountByBlockHashRequest] with JsonEncoder[GetUncleCountByBlockHashResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetUncleCountByBlockHashRequest] =
      params match {
        case Some(JArray(JString(hash) :: Nil)) =>
          for {
            blockHash <- extractHash(hash)
          } yield GetUncleCountByBlockHashRequest(blockHash)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetUncleCountByBlockHashResponse): JValue = encodeAsHex(t.result)
  }

  implicit val eth_getBlockTransactionCountByNumber = new JsonDecoder[GetBlockTransactionCountByNumberRequest] with JsonEncoder[GetBlockTransactionCountByNumberResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetBlockTransactionCountByNumberRequest] =
      params match {
        case Some(JArray((blockValue: JValue) :: Nil)) =>
          for {
            block <- extractBlockParam(blockValue)
          } yield GetBlockTransactionCountByNumberRequest(block)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetBlockTransactionCountByNumberResponse): JValue = encodeAsHex(t.result)
  }

  implicit val eth_getBalance = new JsonDecoder[GetBalanceRequest] with JsonEncoder[GetBalanceResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetBalanceRequest] =
      params match {
        case Some(JArray((addressStr: JString) :: (blockValue: JValue) :: Nil)) =>
          for {
            address <- extractAddress(addressStr)
            block <- extractBlockParam(blockValue)
          } yield GetBalanceRequest(address, block)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetBalanceResponse): JValue = encodeAsHex(t.value)
  }

  implicit val eth_getStorageAt = new JsonDecoder[GetStorageAtRequest] with JsonEncoder[GetStorageAtResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetStorageAtRequest] =
      params match {
        case Some(JArray((addressStr: JString) :: (positionStr: JString) :: (blockValue: JValue) :: Nil)) =>
          for {
            address <- extractAddress(addressStr)
            position <- extractQuantity(positionStr)
            block <- extractBlockParam(blockValue)
          } yield GetStorageAtRequest(address, position, block)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetStorageAtResponse): JValue = encodeAsHex(t.value)
  }

  implicit val eth_getTransactionCount = new JsonDecoder[GetTransactionCountRequest] with JsonEncoder[GetTransactionCountResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetTransactionCountRequest] =
      params match {
        case Some(JArray((addressStr: JString) :: (blockValue: JValue) :: Nil)) =>
          for {
            address <- extractAddress(addressStr)
            block <- extractBlockParam(blockValue)
          } yield GetTransactionCountRequest(address, block)
        case _ => Left(InvalidParams())
      }

    def encodeJson(t: GetTransactionCountResponse): JValue = encodeAsHex(t.value)
  }

  implicit val newFilterResponseEnc = new JsonEncoder[NewFilterResponse] {
    def encodeJson(t: NewFilterResponse): JValue = encodeAsHex(t.filterId)
  }

  implicit val eth_newFilter = new JsonDecoder[NewFilterRequest] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, NewFilterRequest] =
      params match {
        case Some(JArray((filterObj: JObject) :: Nil)) =>
          for {
            filter <- extractFilter(filterObj)
          } yield NewFilterRequest(filter)
        case _ => Left(InvalidParams())
      }
  }

  implicit val eth_newBlockFilter = new JsonDecoder[NewBlockFilterRequest] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, NewBlockFilterRequest] =
      Right(NewBlockFilterRequest())
  }

  implicit val eth_newPendingTransactionFilter = new JsonDecoder[NewPendingTransactionFilterRequest] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, NewPendingTransactionFilterRequest] =
      Right(NewPendingTransactionFilterRequest())
  }

  implicit val eth_uninstallFilter = new JsonDecoder[UninstallFilterRequest] with JsonEncoder[UninstallFilterResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, UninstallFilterRequest] =
      params match {
        case Some(JArray((rawFilterId: JValue) :: Nil)) =>
          for {
            filterId <- extractQuantity(rawFilterId)
          } yield UninstallFilterRequest(filterId)
        case _ => Left(InvalidParams())
      }
    override def encodeJson(t: UninstallFilterResponse): JValue = JBool(t.success)
  }

  implicit val eth_getFilterChanges = new JsonDecoder[GetFilterChangesRequest] with JsonEncoder[GetFilterChangesResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetFilterChangesRequest] =
      params match {
        case Some(JArray((rawFilterId: JValue) :: Nil)) =>
          for {
            filterId <- extractQuantity(rawFilterId)
          } yield GetFilterChangesRequest(filterId)
        case _ => Left(InvalidParams())
      }
    override def encodeJson(t: GetFilterChangesResponse): JValue =
      t.filterChanges match {
        case FilterManager.LogFilterChanges(logs)                    => JArray(logs.map(Extraction.decompose).toList)
        case FilterManager.BlockFilterChanges(blockHashes)           => JArray(blockHashes.map(encodeAsHex).toList)
        case FilterManager.PendingTransactionFilterChanges(txHashes) => JArray(txHashes.map(encodeAsHex).toList)
      }
  }

  implicit val eth_getFilterLogs = new JsonDecoder[GetFilterLogsRequest] with JsonEncoder[GetFilterLogsResponse] {
    import FilterManager._

    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetFilterLogsRequest] =
      params match {
        case Some(JArray((rawFilterId: JValue) :: Nil)) =>
          for {
            filterId <- extractQuantity(rawFilterId)
          } yield GetFilterLogsRequest(filterId)
        case _ => Left(InvalidParams())
      }

    override def encodeJson(t: GetFilterLogsResponse): JValue =
      t.filterLogs match {
        case LogFilterLogs(logs)                    => JArray(logs.map(Extraction.decompose).toList)
        case BlockFilterLogs(blockHashes)           => JArray(blockHashes.map(encodeAsHex).toList)
        case PendingTransactionFilterLogs(txHashes) => JArray(txHashes.map(encodeAsHex).toList)
      }
  }

  implicit val eth_getLogs = new JsonDecoder[GetLogsRequest] with JsonEncoder[GetLogsResponse] {
    def decodeJson(params: Option[JArray]): Either[JsonRpcError, GetLogsRequest] =
      params match {
        case Some(JArray((filterObj: JObject) :: Nil)) =>
          for {
            filter <- extractFilter(filterObj)
          } yield GetLogsRequest(filter)
        case _ => Left(InvalidParams())
      }

    override def encodeJson(t: GetLogsResponse): JValue =
      JArray(t.filterLogs.logs.map(Extraction.decompose).toList)
  }

  private def extractFilter(obj: JObject): Either[JsonRpcError, Filter] = {
    def allSuccess[T](eithers: List[Either[JsonRpcError, T]]): Either[JsonRpcError, List[T]] = {
      if (eithers.forall(_.isRight)) Right(eithers.map(_.right.get))
      else Left(InvalidParams(msg = eithers.collect { case Left(err) => err.message }.mkString("\n")))
    }

    def parseTopic(jstr: JString): Either[JsonRpcError, ByteString] = {
      extractBytes(jstr).left.map(_ => InvalidParams(msg = s"Unable to parse topics, expected byte data but got ${jstr.values}"))
    }

    def parseNestedTopics(jarr: JArray): Either[JsonRpcError, List[ByteString]] = {
      allSuccess(jarr.arr.map {
        case jstr: JString => parseTopic(jstr)
        case other         => Left(InvalidParams(msg = s"Unable to parse topics, expected byte data but got: $other"))
      })
    }

    val topicsEither: Either[JsonRpcError, List[List[ByteString]]] =
      allSuccess((obj \ "topics").extractOpt[JArray].map(_.arr).getOrElse(Nil).map {
        case JNull         => Right(Nil)
        case jstr: JString => parseTopic(jstr).map(List(_))
        case jarr: JArray  => parseNestedTopics(jarr)
        case other         => Left(InvalidParams(msg = s"Unable to parse topics, expected byte data or array but got: $other"))
      })

    def optionalBlockParam(field: String) =
      (obj \ field).extractOpt[JValue].flatMap {
        case JNothing => None
        case other    => Some(extractBlockParam(other))
      }

    for {
      fromBlock <- toEitherOpt(optionalBlockParam("fromBlock"))
      toBlock <- toEitherOpt(optionalBlockParam("toBlock"))
      address <- toEitherOpt((obj \ "address").extractOpt[String].map(extractAddress))
      topics <- topicsEither
    } yield Filter(
      fromBlock = fromBlock,
      toBlock = toBlock,
      address = address,
      topics = topics
    )
  }

  implicit val eth_sign = new JsonDecoder[SignRequest] {
    override def decodeJson(params: Option[JArray]): Either[JsonRpcError, SignRequest] =
      params match {
        case Some(JArray(JString(addr) :: JString(message) :: _)) =>
          for {
            message <- extractBytes(message)
            address <- extractAddress(addr)
          } yield SignRequest(message, address, None)
        case _ =>
          Left(InvalidParams())
      }
  }

  def extractCall(obj: JObject): Either[JsonRpcError, CallTx] = {
    def toEitherOpt[A, B](opt: Option[Either[A, B]]): Either[A, Option[B]] =
      opt.map(_.right.map(Some.apply)).getOrElse(Right(None))

    def optionalQuantity(input: JValue): Either[JsonRpcError, Option[DataWord]] =
      input match {
        case JNothing => Right(None)
        case o        => extractQuantity(o).map(Some(_))
      }

    for {
      from <- toEitherOpt((obj \ "from").extractOpt[String].map(extractBytes))
      to <- toEitherOpt((obj \ "to").extractOpt[String].map(extractBytes))
      gas <- optionalQuantity(obj \ "gas")
      gasPrice <- optionalQuantity(obj \ "gasPrice")
      value <- optionalQuantity(obj \ "value")
      data <- toEitherOpt((obj \ "data").extractOpt[String].map(extractBytes))
    } yield CallTx(
      from = from,
      to = to,
      gas = gas.map(_.longValue),
      gasPrice = gasPrice.getOrElse(DataWord.Zero),
      value = value.getOrElse(DataWord.Zero),
      data = data.getOrElse(ByteString(""))
    )
  }

}
