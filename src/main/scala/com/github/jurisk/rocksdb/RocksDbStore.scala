package com.github.jurisk.rocksdb

import java.io.File

import akka.util.ByteString
import com.github.jurisk.filecache.FileCache
import org.rocksdb._
import org.rocksdb.util.SizeUnit

import scala.concurrent.blocking
import scala.concurrent.{ExecutionContext, Future}

class RocksDbStore(path: String, ttlSeconds: Option[Int]) extends FileCache {
  private var isOpen = false

  RocksDB.loadLibrary()

  private val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(256 * SizeUnit.MB)
    .setMaxWriteBufferNumber(4)
    .setMaxBackgroundCompactions(16)
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  private val store = ttlSeconds map { ttl =>
    TtlDB.open(options, path, ttl, false)
  } getOrElse {
    RocksDB.open(options, path)
  }

  isOpen = true

  override def getByByteKey(key: Array[Byte], updateTtl: Boolean)(implicit ec: ExecutionContext): Future[ByteString] = {
    assert(isOpen)
    Future {
      blocking {
        val result = Some(store.get(key)).map(ByteString.apply).getOrElse(sys.error(s"Failed to find entry for $key"))
        if (updateTtl) { // we update TTL by writing it again
          putByByteKey(key, result)
        }
        result
      }
    }
  }

  override def putByByteKey(key: Array[Byte], byteString: ByteString)(implicit ec: ExecutionContext): Future[Unit] = {
    assert(isOpen)
    Future {
      blocking {
        store.put(key, byteString.toArray)
      }
    }
  }

  override def deleteByByteKey(key: Array[Byte])(implicit ec: ExecutionContext): Future[Unit] = {
    assert(isOpen)
    Future {
      blocking {
        store.delete(key)
      }
    }
  }

  override def deleteDatabase(): Unit = {
    val file = new File(path)
    file.delete()
  }

  def shutdown(): Unit = {
    store.close()
    isOpen = false
  }
}
