package com.github.jurisk.rocksdb

import java.io.File
import java.util.UUID

object RocksDbTestApp extends App {
  val tmpFile = File.createTempFile("rocksdb", ".db")
  val tmpFileName = tmpFile.getAbsolutePath
  tmpFile.delete

  val store = new RocksDbStore(tmpFileName)

  private def random(): String = {
    UUID.randomUUID().toString
  }

  (0 to 1000) foreach { _ =>
    val key = random()
    val value = random()

    store.putString(key, value)
    val result = store.getString(key)

    require(result.contains(value), s"$result does not equal $value")
  }

  tmpFile.delete
}
