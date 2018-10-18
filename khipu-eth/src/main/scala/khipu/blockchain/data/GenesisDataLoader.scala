package khipu.blockchain.data

import akka.actor.ActorSystem
import akka.event.Logging
import akka.util.ByteString
import java.io.FileNotFoundException
import java.math.BigInteger
import khipu.Hash
import khipu.crypto
import khipu.UInt256
import khipu.domain.{ Account, Block, BlockHeader, Blockchain }
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.rlp
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.store.datasource.DataSource
import khipu.store.datasource.EphemDataSource
import khipu.store.trienode.ArchiveNodeStorage
import khipu.store.trienode.NodeStorage
import khipu.store.trienode.PruningMode
import khipu.trie
import khipu.trie.MerklePatriciaTrie
import khipu.util.BlockchainConfig
import khipu.util.Config.DbConfig
import org.json4s.{ CustomSerializer, DefaultFormats, JString, JValue }
import scala.io.Source
import scala.util.{ Failure, Success, Try }

object GenesisDataLoader {

  object JsonSerializers {
    def deserializeByteString(jv: JValue): ByteString = jv match {
      case JString(s) =>
        val noPrefix = s.replace("0x", "")
        val inp = if (noPrefix.length % 2 == 0) {
          noPrefix
        } else {
          "0" ++ noPrefix
        }
        Try(ByteString(khipu.hexDecode(inp))) match {
          case Success(bs) => bs
          case Failure(ex) => throw new RuntimeException("Cannot parse hex string: " + s)
        }
      case other => throw new RuntimeException("Expected hex string, but got: " + other)
    }

    def deserializeHash(jv: JValue): Hash = jv match {
      case JString(s) =>
        val noPrefix = s.replace("0x", "")
        val inp = if (noPrefix.length % 2 == 0) {
          noPrefix
        } else {
          "0" ++ noPrefix
        }
        Try(Hash(khipu.hexDecode(inp))) match {
          case Success(bs) => bs
          case Failure(ex) => throw new RuntimeException("Cannot parse hex string: " + s)
        }
      case other => throw new RuntimeException("Expected hex string, but got: " + other)
    }

    object ByteStringJsonSerializer extends CustomSerializer[ByteString](formats => (
      { case jv => deserializeByteString(jv) },
      PartialFunction.empty
    ))

    object HashJsonSerializer extends CustomSerializer[Hash](formats => (
      { case jv => deserializeHash(jv) },
      PartialFunction.empty
    ))
  }
}
class GenesisDataLoader(
    dataSource:       DataSource,
    blockchain:       Blockchain,
    pruningMode:      PruningMode,
    blockchainConfig: BlockchainConfig,
    dbConfig:         DbConfig
)(implicit system: ActorSystem) {
  import GenesisDataLoader._
  private val log = Logging(system, this.getClass)

  private val bloomLength = 512
  private val hashLength = 64
  private val addressLength = 40

  private val emptyTrieRootHash = Hash(crypto.kec256(rlp.encode(Array.emptyByteArray)))
  private val emptyEvmHash: Hash = Hash(crypto.kec256(Array.emptyByteArray))

  def loadGenesisData(): Unit = {
    log.debug("Loading genesis data")

    val genesisJson = blockchainConfig.customGenesisFileOpt match {
      case Some(customGenesisFile) =>
        log.debug(s"Trying to load custom genesis data from file: $customGenesisFile")

        Try(Source.fromFile(customGenesisFile)).recoverWith {
          case _: FileNotFoundException =>
            log.debug(s"Cannot load custom genesis data from file: $customGenesisFile")
            log.debug(s"Trying to load from resources: $customGenesisFile")
            Try(Source.fromResource(customGenesisFile))
        } match {
          case Success(customGenesis) =>
            log.debug(s"Using custom genesis data from: $customGenesisFile")
            try {
              customGenesis.getLines().mkString
            } finally {
              customGenesis.close()
            }
          case Failure(ex) =>
            log.error(ex, s"Cannot load custom genesis data from: $customGenesisFile")
            throw ex
        }
      case None =>
        log.debug(s"Using default genesis data")
        val src = Source.fromResource("blockchain/default-genesis.json")
        try {
          src.getLines().mkString
        } finally {
          src.close()
        }
    }

    loadGenesisData(genesisJson) match {
      case Success(_) =>
        log.debug("Genesis data successfully loaded")
      case Failure(ex) =>
        log.error(ex, "Unable to load genesis data")
        throw ex
    }
  }

  private def loadGenesisData(genesisJson: String): Try[Unit] = {
    implicit val formats = DefaultFormats + JsonSerializers.ByteStringJsonSerializer + JsonSerializers.HashJsonSerializer
    for {
      genesisData <- Try(org.json4s.native.JsonMethods.parse(genesisJson).extract[GenesisData])
      _ <- loadGenesisData(genesisData)
    } yield ()
  }

  private def loadGenesisData(genesisData: GenesisData): Try[Unit] = {
    val ephemDataSource = EphemDataSource()
    val nodeStorage = new NodeStorage(ephemDataSource)
    val initalRootHash = trie.EmptyTrieHash

    val stateMptRootHash = genesisData.alloc.zipWithIndex.foldLeft(initalRootHash) {
      case (rootHash, (((address, AllocAccount(balance)), idx))) =>
        val ephemNodeStorage = new ArchiveNodeStorage(nodeStorage)
        val mpt = MerklePatriciaTrie[Array[Byte], Account](rootHash, ephemNodeStorage)(trie.byteArraySerializable, Account.accountSerializer)
        val paddedAddress = address.reverse.padTo(addressLength, "0").reverse.mkString
        val account = Account(blockchainConfig.accountStartNonce, UInt256(new BigInteger(balance)), emptyTrieRootHash, emptyEvmHash)

        mpt.put(crypto.kec256(khipu.hexDecode(paddedAddress)), account).persist().rootHash
    }

    val header = BlockHeader(
      parentHash = Hash(zeros(hashLength)),
      ommersHash = Hash(crypto.kec256(rlp.encode(RLPList()))),
      beneficiary = genesisData.coinbase,
      stateRoot = Hash(stateMptRootHash),
      transactionsRoot = emptyTrieRootHash,
      receiptsRoot = emptyTrieRootHash,
      logsBloom = ByteString(zeros(bloomLength)),
      difficulty = UInt256(new BigInteger(genesisData.difficulty.replace("0x", ""), 16)),
      number = 0,
      gasLimit = new BigInteger(genesisData.gasLimit.replace("0x", ""), 16).longValue,
      gasUsed = 0,
      unixTimestamp = new BigInteger(genesisData.timestamp.replace("0x", ""), 16).longValue,
      extraData = genesisData.extraData,
      mixHash = genesisData.mixHash,
      nonce = genesisData.nonce
    )

    log.debug(s"prepared genesis header: $header")

    blockchain.getBlockHeaderByNumber(0) match {
      case Some(existingGenesisHeader) if existingGenesisHeader.hash == header.hash =>
        log.debug("Genesis data already in the database")
        Success(())
      case Some(existingGenesisHeader) =>
        log.error(s"existingGenesisHeader $existingGenesisHeader vs header ${header}, hash: ${existingGenesisHeader.hash} vs ${header.hash}")
        Failure(new RuntimeException("Genesis data present in the database does not match genesis block from file." +
          " Use different directory for running private blockchains."))
      case None =>
        // using empty namespace because ephemDataSource.storage already has the namespace-prefixed keys
        // TODO deal it with kesque, otherwise the regular sync from scrath won't work because of lack these initial nodes
        ephemDataSource.toSeq.grouped(dbConfig.batchSize).foreach(toStore => dataSource.update(Array.emptyByteArray, Nil, toStore))
        blockchain.saveBlock(Block(header, BlockBody(Nil, Nil)))
        blockchain.saveReceipts(header.hash, Nil)
        blockchain.saveTotalDifficulty(header.hash, header.difficulty)
        Success(())
    }
  }

  private def zeros(length: Int): Array[Byte] =
    khipu.hexDecode(List.fill(length)("0").mkString)

}

