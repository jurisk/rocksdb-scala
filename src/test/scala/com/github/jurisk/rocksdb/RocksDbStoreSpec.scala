package com.github.jurisk.rocksdb

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID
import akka.util.ByteString
import com.github.jurisk.filecache.FileCache
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

class RocksDbStoreSpec extends FlatSpec with BeforeAndAfterAll {
  private val UTF8 = StandardCharsets.UTF_8.name

  private val tmpFile = File.createTempFile("rocksdb", ".db")
  private val tmpFileName = tmpFile.getAbsolutePath
  tmpFile.delete

  val store: FileCache = new RocksDbStore(tmpFileName)

  private def random(): String = {
    UUID.randomUUID().toString
  }

  def putSync(key: String, value: String): Unit = {
    val f = store.put(key, ByteString(value.getBytes(UTF8)))
    Await.result(f, 1.second)
  }

  def getSync(key: String): String = {
    val f = store.get(key)
    val result = Await.result(f, 1.second)
    result.utf8String
  }

  def deleteSync(key: String): Unit = {
    val f = store.delete(key)
    Await.result(f, 1.second)
  }

  it should "put get and delete" in {
    (0 to 1000) foreach { _ =>
      val key = random()
      val value = random()

      putSync(key, value)
      val result = getSync(key)

      require(result == value, s"$result does not equal $value")

      deleteSync(key)
      assertThrows[Exception] {
        getSync(key)
      }
    }
  }

  it should "honour TTL" in {
    // TODO
  }

  override protected def afterAll(): Unit = {
    tmpFile.delete
  }
}