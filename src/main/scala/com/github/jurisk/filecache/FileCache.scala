package com.github.jurisk.filecache

import java.nio.charset.StandardCharsets

import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

trait FileCache {
  private val UTF8 = StandardCharsets.UTF_8.name

  def getByByteKey(key: Array[Byte], updateTtl: Boolean)(implicit ec: ExecutionContext): Future[Option[ByteString]]

  def get(key: String, updateTtl: Boolean)(implicit ec: ExecutionContext): Future[Option[ByteString]] = {
    getByByteKey(key.getBytes(UTF8), updateTtl)
  }

  def putByByteKey(key: Array[Byte], byteString: ByteString)(implicit ec: ExecutionContext): Future[Unit]

  def put(key: String, byteString: ByteString, updateTtl: Boolean = false)(implicit ec: ExecutionContext): Future[Unit] = {
    putByByteKey(key.getBytes(UTF8), byteString)
  }

  def deleteByByteKey(key: Array[Byte])(implicit ec: ExecutionContext): Future[Unit]

  def delete(key: String)(implicit ec: ExecutionContext): Future[Unit] = {
    deleteByByteKey(key.getBytes(UTF8))
  }

  def compact(): Unit

  def deleteDatabase(): Unit
}