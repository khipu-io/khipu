## Khipu: A Scala/Akka implementation of the Ethereum protocol

Khipu is developed and maintained by khipu.io Team.
It is built on earlier work on Mantis by [Grothendieck Team](https://iohk.io/projects/ethereum-classic/), but supports ETH.

The major researches of Khipu so far:

  - Try to execute transactions in a block as in parallel as possible. Average 80% transactions in one block could be executed in parallel currently
  - A storage engine (Kesque) specially designed for blockchain, developed based on Kafka's log engine. Which perform only 1 disk I/O at most for 99.x% random read

### Status - Alpha Release 0.1.0-alpha

This version of the code supports

  - Peer discovery
  - Fast sync (download a recent state trie snapshot and all blocks, this is the default behaviour)
  - Regular sync (download and execute every transaction in every block in the chain), this will be enabled once fast sync finished

Features to be done

  - Reduce disk usage
  - Reduce memory usage
  - CPU mining
  - Execute transactions in parallel in mining
  - JSON RPC API (useful for console and Mist integration)
  - Morden testnet and private network
  - Unit tests

#### Notice

This version's data storage format may be changed during productional release.

During fast sync, sometimes the syncing looks like stopped with no more state nodes or blocks being downloaded. A possible reason that may be the current left handshaked peers could not respond to state nodes or blocks request any more. In case of this, try to stop khipu and restart it again.


### Minimum requirements to run Khipu

  - 16G RAM, 250G disk space (SSD is preferred, although HDD is okay)
  - Under regular sync mode, if you restart Khipu, it may take 2 to 5 minutes (SSD), or 8 to 15 minutes (HDD) to load the storage indexes

### Installation and Running, Building

The latest release can be downloaded from [here](https://github.com/khipu-io/khipu/releases)

Running from command line:


```
unzip khipu-eth-0.1.0.zip
cd khipu-eth-0.1.0/bin
./khipu-eth
```
or
```
nohup ./khipu-eth &
tail -f nohup
```

Khipu data directory is $HOME/.khipu.eth

```
$ ls .khipu.eth
kesque.logs  keystore  leveldb  nodeId.keys
```

Remove leveldb and kesque.logs will level a installation with empty blockchain data, but the keystore and nodeId.keys will be kept.


#### Prerequisites to build

- JDK 1.8 (download from [java.com](http://www.java.com))
- sbt ([download sbt](http://www.scala-sbt.org/download.html))

#### Build the client

As an alternative to downloading the client, build the client from source.

```
git clone https://github.com/khipu-io/khipu.git
cd khipu
sbt khipu-eth/dist
```
or
```
sbt clean khipu-eth/dist
```

The packaged zip file could be found at `khipu/khipu-eth/target/universal`

## License

Khipu is licensed under the MIT License (found in the LICENSE file in the root directory).

