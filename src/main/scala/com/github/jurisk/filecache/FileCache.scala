package com.github.jurisk.filecache

import java.nio.charset.StandardCharsets

import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

trait FileCache {
  private val UTF8 = StandardCharsets.UTF_8.name

  def getByByteKey(key: Array[Byte])(implicit ec: ExecutionContext): Future[ByteString]

  def get(key: String)(implicit ec: ExecutionContext): Future[ByteString] = {
    getByByteKey(key.getBytes(UTF8))
  }

  def putByByteKey(key: Array[Byte], byteString: ByteString)(implicit ec: ExecutionContext): Future[Unit]

  def put(key: String, byteString: ByteString)(implicit ec: ExecutionContext): Future[Unit] = {
    putByByteKey(key.getBytes(UTF8), byteString)
  }

  def deleteByByteKey(key: Array[Byte])(implicit ec: ExecutionContext): Future[Unit]

  def delete(key: String)(implicit ec: ExecutionContext): Future[Unit] = {
    deleteByByteKey(key.getBytes(UTF8))
  }
}