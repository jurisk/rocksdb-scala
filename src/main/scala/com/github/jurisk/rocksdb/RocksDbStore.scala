package com.github.jurisk.rocksdb

import akka.util.ByteString
import com.github.jurisk.filecache.FileCache
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import scala.concurrent.blocking
import scala.concurrent.{ExecutionContext, Future}

class RocksDbStore(fileName: String) extends FileCache {
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

  override def getByByteKey(key: Array[Byte])(implicit ec: ExecutionContext): Future[ByteString] = {
    assert(isOpen)
    Future {
      blocking {
        Some(store.get(key)).map(ByteString.apply).getOrElse(sys.error(s"Failed to find entry for $key"))
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

  def shutdown(): Unit = {
    store.close()
    isOpen = false
  }
}
