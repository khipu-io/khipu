import java.io.PrintWriter

import sbt.TaskKey
import sbt._
import Keys._

object SolidityPlugin extends AutoPlugin {

  object autoImport {
    lazy val solidityCompile = TaskKey[Unit]("solidityCompile", "Compiles solidity contracts")
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    solidityCompile := {
      import sys.process._

      val contractsDir = baseDirectory.value / "src" / "evmTest" / "resources" / "solidity"
      val outDir = baseDirectory.value / "target" / "contracts"

      (contractsDir ** "*.sol").get.foreach { f =>
        Seq("solc", f.getPath, "--bin", "--overwrite", "-o", outDir.getPath).!!

        // this is a temporary workaround, see: https://github.com/ethereum/solidity/issues/1732
        val abiOut = Seq("solc", f.getPath, "--abi").!!
        val abisLines = abiOut.split("\n").sliding(4, 4)
        abisLines.foreach { abiLines =>
          val contractName = abiLines(1)
            .replace(f.getPath, "")
            .dropWhile(_ != ':').drop(1)
            .takeWhile(_ != ' ')
          new PrintWriter(outDir / s"$contractName.abi") {
            write(abiLines.drop(3).mkString); close()
          }
        }
      }
    }
  )

}
