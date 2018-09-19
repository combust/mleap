package ml.bundle.hdfs

import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSpec

class HadoopBundleFileSystemSpec extends FunSpec {
  private val fs = FileSystem.get(new Configuration())
  private val bundleFs = new HadoopBundleFileSystem(fs)

  describe("scheme") {
    it("returns hdfs") {
      assert(bundleFs.schemes == Seq("hdfs"))
    }
  }

  describe("load") {
    it("loads a file from hadoop and saves to a local file") {
      val testFile = Files.createTempFile("HadoopBundleFileSystemSpec", ".txt")
      Files.write(testFile.toAbsolutePath, "HELLO".getBytes())

      val loadedFile = bundleFs.load(testFile.toUri).get
      val contents = new String(Files.readAllBytes(loadedFile.toPath))

      assert(contents == "HELLO")
    }
  }

  describe("save") {
    it("saves local file to HDFS") {
      val testFile = Files.createTempFile("HadoopBundleFileSystemSpec", ".txt")
      Files.write(testFile.toAbsolutePath, "HELLO".getBytes())

      val tmpDir = Files.createTempDirectory("HadoopBundleFileSystemSpec")
      val tmpFile = new URI(s"file://$tmpDir/test.txt")

      bundleFs.save(tmpFile, testFile.toFile)
      val contents = new String(Files.readAllBytes(Paths.get(tmpFile)))

      assert(contents == "HELLO")
    }
  }
}
