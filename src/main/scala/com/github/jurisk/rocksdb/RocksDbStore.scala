package com.github.jurisk.rocksdb

import java.nio.charset.StandardCharsets

import org.rocksdb._
import org.rocksdb.util.SizeUnit

class RocksDbStore(fileName: String) {
  private val UTF8 = StandardCharsets.UTF_8.name
  private var isOpen = false

  RocksDB.loadLibrary()

  private val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(256 * SizeUnit.MB)
    .setMaxWriteBufferNumber(4)
    .setMaxBackgroundCompactions(16)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  private val store = RocksDB.open(options, fileName )
  isOpen = true

  def putBytes(key: Array[Byte], value: Array[Byte]): Unit = {
    assert(isOpen)
    store.put(key, value)
  }

  def putString(key: String, value: String): Unit = {
    putBytes(key.getBytes(UTF8), value.getBytes(UTF8))
  }

  def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    assert(isOpen)
    Some(store.get(key))
  }

  def getString(key: String): Option[String]= {
    getBytes(key.getBytes(UTF8)).map(new String(_, UTF8))
  }

  def shutdown(): Unit = {
    store.close()
    isOpen = false
  }
}
