package khipu.jsonrpc

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.{ ByteString, Timeout }
import khipu.Hash
import khipu.DataWord
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.domain.{ Account, Address, Blockchain }
import khipu.jsonrpc.PersonalService._
import khipu.keystore.{ KeyStore, Wallet }
import khipu.jsonrpc.JsonRpcErrors._
import khipu.store.AppStateStorage
import khipu.transactions.PendingTransactionsService
import khipu.util.BlockchainConfig
import khipu.util.TxPoolConfig
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

object PersonalService {

  final case class ImportRawKeyRequest(prvKey: ByteString, passphrase: String)
  final case class ImportRawKeyResponse(address: Address)

  final case class NewAccountRequest(passphrase: String)
  final case class NewAccountResponse(address: Address)

  final case class ListAccountsRequest()
  final case class ListAccountsResponse(addresses: List[Address])

  final case class UnlockAccountRequest(address: Address, passphrase: String)
  final case class UnlockAccountResponse(result: Boolean)

  final case class LockAccountRequest(address: Address)
  final case class LockAccountResponse(result: Boolean)

  final case class SendTransactionWithPassphraseRequest(tx: TransactionRequest, passphrase: String)
  final case class SendTransactionWithPassphraseResponse(txHash: Hash)

  final case class SendTransactionRequest(tx: TransactionRequest)
  final case class SendTransactionResponse(txHash: Hash)

  final case class SignRequest(message: ByteString, address: Address, passphrase: Option[String])
  final case class SignResponse(signature: ECDSASignature)

  final case class EcRecoverRequest(message: ByteString, signature: ECDSASignature)
  final case class EcRecoverResponse(address: Address)

  val InvalidKey = InvalidParams("Invalid key provided, expected 32 bytes (64 hex digits)")
  val InvalidAddress = InvalidParams("Invalid address, expected 20 bytes (40 hex digits)")
  val InvalidPassphrase = LogicError("Could not decrypt key with given passphrase")
  val KeyNotFound = LogicError("No key found for the given address")

  val PrivateKeyLength = 32
}

class PersonalService(
    keyStore:         KeyStore.I,
    blockchain:       Blockchain,
    appStateStorage:  AppStateStorage,
    blockchainConfig: BlockchainConfig,
    txPoolConfig:     TxPoolConfig
)(implicit system: ActorSystem) {

  private val unlockedWallets: mutable.Map[Address, Wallet] = mutable.Map.empty

  def pendingTransactionsService = PendingTransactionsService.proxy(system)

  def importRawKey(req: ImportRawKeyRequest): ServiceResponse[ImportRawKeyResponse] = Future {
    for {
      prvKey <- Right(req.prvKey).filterOrElse(_.length == PrivateKeyLength, InvalidKey)
      addr <- keyStore.importPrivateKey(prvKey, req.passphrase).left.map(handleError)
    } yield ImportRawKeyResponse(addr)
  }

  def newAccount(req: NewAccountRequest): ServiceResponse[NewAccountResponse] = Future {
    keyStore.newAccount(req.passphrase)
      .map(NewAccountResponse.apply)
      .left.map(handleError)
  }

  def listAccounts(request: ListAccountsRequest): ServiceResponse[ListAccountsResponse] = Future {
    keyStore.listAccounts()
      .map(ListAccountsResponse.apply)
      .left.map(handleError)
  }

  def unlockAccount(request: UnlockAccountRequest): ServiceResponse[UnlockAccountResponse] = Future {
    keyStore.unlockAccount(request.address, request.passphrase)
      .left.map(handleError)
      .map { wallet =>
        unlockedWallets += request.address -> wallet
        UnlockAccountResponse(true)
      }
  }

  def lockAccount(request: LockAccountRequest): ServiceResponse[LockAccountResponse] = Future.successful {
    unlockedWallets -= request.address
    Right(LockAccountResponse(true))
  }

  def sign(request: SignRequest): ServiceResponse[SignResponse] = Future {
    import request._

    val accountWallet = {
      if (passphrase.isDefined) keyStore.unlockAccount(address, passphrase.get).left.map(handleError)
      else unlockedWallets.get(request.address).toRight(AccountLocked)
    }

    accountWallet
      .map { wallet =>
        SignResponse(ECDSASignature.sign(getMessageToSign(message), wallet.keyPair))
      }
  }

  def ecRecover(req: EcRecoverRequest): ServiceResponse[EcRecoverResponse] = Future {
    import req._
    ECDSASignature.recoverPublicKey(signature, getMessageToSign(message)).map { publicKey =>
      Right(EcRecoverResponse(Address(crypto.kec256(publicKey))))
    }.getOrElse(Left(InvalidParams("unable to recover address")))
  }

  def sendTransaction(request: SendTransactionWithPassphraseRequest): ServiceResponse[SendTransactionWithPassphraseResponse] = {
    val maybeWalletUnlocked = Future { keyStore.unlockAccount(request.tx.from, request.passphrase).left.map(handleError) }
    maybeWalletUnlocked.flatMap {
      case Right(wallet) =>
        val futureTxHash = sendTransaction(request.tx, wallet)
        futureTxHash.map(txHash => Right(SendTransactionWithPassphraseResponse(txHash)))
      case Left(err) => Future.successful(Left(err))
    }
  }

  def sendTransaction(request: SendTransactionRequest): ServiceResponse[SendTransactionResponse] = {
    unlockedWallets.get(request.tx.from) match {
      case Some(wallet) =>
        val futureTxHash = sendTransaction(request.tx, wallet)
        futureTxHash.map(txHash => Right(SendTransactionResponse(txHash)))

      case None =>
        Future.successful(Left(AccountLocked))
    }
  }

  private def sendTransaction(request: TransactionRequest, wallet: Wallet): Future[Hash] = {
    implicit val timeout = Timeout(txPoolConfig.pendingTxManagerQueryTimeout)

    val pendingTxsFuture = (pendingTransactionsService ? PendingTransactionsService.GetPendingTransactions).mapTo[PendingTransactionsService.PendingTransactionsResponse]
    val latestPendingTxNonceFuture: Future[Option[DataWord]] = pendingTxsFuture.map { pendingTxs =>
      val senderTxsNonces = pendingTxs.pendingTransactions
        .collect { case ptx if ptx.stx.sender == wallet.address => ptx.stx.tx.nonce }
      Try(senderTxsNonces.max).toOption
    }
    latestPendingTxNonceFuture.map { maybeLatestPendingTxNonce =>
      val maybeCurrentNonce = getCurrentAccount(request.from).map(_.nonce)
      val maybeNextTxNonce = maybeLatestPendingTxNonce.map(_ + DataWord.Zero) orElse maybeCurrentNonce
      val tx = request.toTransaction(maybeNextTxNonce.getOrElse(blockchainConfig.accountStartNonce))

      val stx = if (appStateStorage.getBestBlockNumber >= blockchainConfig.eip155BlockNumber) {
        wallet.signTx(tx, Some(blockchainConfig.chainId))
      } else {
        wallet.signTx(tx, None)
      }

      pendingTransactionsService ! PendingTransactionsService.AddOrOverrideTransaction(stx)

      stx.hash
    }
  }

  private def getCurrentAccount(address: Address): Option[Account] =
    blockchain.getAccount(address, appStateStorage.getBestBlockNumber)

  private def getMessageToSign(message: ByteString) = {
    val prefixed: Array[Byte] =
      0x19.toByte +: s"Ethereum Signed Message:\n${message.length}".getBytes ++: message.toArray[Byte]

    crypto.kec256(prefixed)
  }

  private val handleError: PartialFunction[KeyStore.KeyStoreError, JsonRpcError] = {
    case KeyStore.DecryptionFailed => InvalidPassphrase
    case KeyStore.KeyNotFound      => KeyNotFound
    case KeyStore.IOError(msg)     => LogicError(msg)
  }
}
