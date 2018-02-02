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
import org.scalatest.time.{Seconds, Span}

class RocksDbStoreSpec extends FlatSpec {
  private val UTF8 = StandardCharsets.UTF_8.name

  private def createStore(ttlSeconds: Option[Int]): FileCache = {
    val tmpFile = File.createTempFile("rocksdb", ".db")
    val tmpFileName = tmpFile.getAbsolutePath
    tmpFile.delete

    new RocksDbStore(tmpFileName, ttlSeconds)
  }

  private def random(): String = {
    UUID.randomUUID().toString
  }

  private def putSync(store: FileCache, key: String, value: String): Unit = {
    val f = store.put(key, ByteString(value.getBytes(UTF8)))
    Await.result(f, 1.second)
  }

  private def getSync(store: FileCache, key: String, updateTtl: Boolean): Option[String] = {
    val f = store.get(key, updateTtl)
    val result = Await.result(f, 1.second)
    result.map(_.utf8String)
  }

  private def deleteSync(store: FileCache, key: String): Unit = {
    val f = store.delete(key)
    Await.result(f, 1.second)
  }

  private def randomData(size: Int): Map[String, String] = {
    ((0 to size) map { _ =>
      random() -> random()
    }).toMap
  }

  private def checkData(store: FileCache, data: Map[String, String], updateTtl: Boolean): Unit = {
    data foreach { case (k, v) =>
      val result = getSync(store, k, updateTtl = updateTtl)
      require(result.contains(v), s"$result does not equal $v")
    }
  }

  it should "put, get and delete" in {
    val store = createStore(ttlSeconds = None)
    val data = randomData(1024)

    data foreach { case (k, v) =>
      putSync(store, k, v)
    }

    checkData(store, data, updateTtl = true)

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

    (0 to 5) foreach { _ =>
      store.compact() // this should not clear data yet as not enough time has passed since last TTL refreshes
      checkData(store, data, updateTtl = true)
      Thread.sleep(500) // half of TTL
    }

    eventually(timeout(Span(30, Seconds))) { // however eventually they should get cleared
      store.compact()

      data.keys foreach { k =>
        val result = getSync(store, k, updateTtl = false)
        require(result.isEmpty, s"Expected empty but got $result")
      }
    }
  }
}