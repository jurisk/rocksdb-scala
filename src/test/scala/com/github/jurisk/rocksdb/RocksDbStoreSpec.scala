package com.github.jurisk.rocksdb

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.util.ByteString
import com.github.jurisk.filecache.FileCache
import org.scalatest.FlatSpec

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Minutes, Span}

class RocksDbStoreSpec extends FlatSpec {
  private val UTF8 = StandardCharsets.UTF_8.name

  def createStore(ttlSeconds: Option[Int]): FileCache = {
    val tmpFile = File.createTempFile("rocksdb", ".db")
    val tmpFileName = tmpFile.getAbsolutePath
    tmpFile.delete

    new RocksDbStore(tmpFileName, ttlSeconds)
  }

  private def random(): String = {
    UUID.randomUUID().toString
  }

  def putSync(store: FileCache, key: String, value: String): Unit = {
    val f = store.put(key, ByteString(value.getBytes(UTF8)))
    Await.result(f, 1.second)
  }

  def getSync(store: FileCache, key: String, updateTtl: Boolean): Option[String] = {
    val f = store.get(key, updateTtl)
    val result = Await.result(f, 1.second)
    result.map(_.utf8String)
  }

  def deleteSync(store: FileCache, key: String): Unit = {
    val f = store.delete(key)
    Await.result(f, 1.second)
  }

  def randomData(size: Int): Map[String, String] = {
    ((0 to size) map { _ =>
      random() -> random()
    }).toMap
  }

  it should "put, get and delete" in {
    val store = createStore(ttlSeconds = None)
    val data = randomData(1024)

    data foreach { case (k, v) =>
      putSync(store, k, v)
    }

    data foreach { case (k, v) =>
      val result = getSync(store, k, updateTtl = false)
      require(result.contains(v), s"$result does not equal $v")
    }

    data.keys foreach { k =>
      deleteSync(store, k)
    }

    data.keys foreach { k =>
      val result = getSync(store, k, updateTtl = false)
      require(result.isEmpty, s"Expected empty but got $result")
    }

    store.deleteDatabase()
  }

  it should "honour TTL" in {
    val ttl = 1
    val store = createStore(ttlSeconds = Some(ttl))
    val data = randomData(1024)

    data foreach { case (k, v) =>
      putSync(store, k, v)
    }

    data foreach { case (k, v) =>
      val result = getSync(store, k, updateTtl = true)
      require(result.contains(v), s"$result does not equal $v")
    }

    eventually(timeout(Span(10, Minutes))) {
      data.keys foreach { k =>
        val result = getSync(store, k, updateTtl = false)
        require(result.isEmpty, s"Expected empty but got $result")
      }
    }
  }
}