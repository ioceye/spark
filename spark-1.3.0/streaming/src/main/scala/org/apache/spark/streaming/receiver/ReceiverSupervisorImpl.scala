/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await

import akka.actor.{Actor, Props}
import akka.pattern.ask
import com.google.common.base.Throwables
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Logging, SparkEnv, SparkException}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Concrete implementation of [[org.apache.spark.streaming.receiver.ReceiverSupervisor]]
 * which provides all the necessary functionality for handling the data received by
 * the receiver. Specifically, it creates a [[org.apache.spark.streaming.receiver.BlockGenerator]]
 * object that is used to divide the received data stream into blocks of data.
 */
private[streaming] class ReceiverSupervisorImpl(
    receiver: Receiver[_],
    env: SparkEnv,
    hadoopConf: Configuration,
    checkpointDirOption: Option[String]
  ) extends ReceiverSupervisor(receiver, env.conf) with Logging {

  private val receivedBlockHandler: ReceivedBlockHandler = {
    // 如果开启了预写日志机制,那么返回WriteAheadLogBasedBlockHandler,否则为BlockManagerBasedBlockHandler
    if (env.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false)) {
      if (checkpointDirOption.isEmpty) {
        throw new SparkException(
          "Cannot enable receiver write-ahead log without checkpoint directory set. " +
            "Please use streamingContext.checkpoint() to set the checkpoint directory. " +
            "See documentation for more details.")
      }
      new WriteAheadLogBasedBlockHandler(env.blockManager, receiver.streamId,
        receiver.storageLevel, env.conf, hadoopConf, checkpointDirOption.get)
    } else {
      new BlockManagerBasedBlockHandler(env.blockManager, receiver.storageLevel)
    }
  }


  /** Remote Akka actor for the ReceiverTracker */
  private val trackerActor = {
    val ip = env.conf.get("spark.driver.host", "localhost")
    val port = env.conf.getInt("spark.driver.port", 7077)
    val url = AkkaUtils.address(
      AkkaUtils.protocol(env.actorSystem),
      SparkEnv.driverActorSystemName,
      ip,
      port,
      "ReceiverTracker")
    env.actorSystem.actorSelection(url)
  }

  /** Timeout for Akka actor messages */
  private val askTimeout = AkkaUtils.askTimeout(env.conf)

  /** Akka actor for receiving messages from the ReceiverTracker in the driver */
  private val actor = env.actorSystem.actorOf(
    Props(new Actor {

      override def receive() = {
        case StopReceiver =>
          logInfo("Received stop signal")
          stop("Stopped by driver", None)
        case CleanupOldBlocks(threshTime) =>
          logDebug("Received delete old batch signal")
          cleanupOldBlocks(threshTime)
      }

      def ref = self
    }), "Receiver-" + streamId + "-" + System.currentTimeMillis())

  /** Unique block ids if one wants to add blocks directly */
  private val newBlockId = new AtomicLong(System.currentTimeMillis())

  /** Divides received data records into data blocks for pushing in BlockManager. */
  private val blockGenerator = new BlockGenerator(new BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    // onPushBlock调用pushArrayBuffer方法推送block
    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }, streamId, env.conf)

  /** Push a single record of received data into block generator. */
  def pushSingle(data: Any) {
    blockGenerator.addData(data)
  }

  /** Store an ArrayBuffer of received data as a data block into Spark's memory. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)
  }

  /** Store a iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(IteratorBlock(iterator), metadataOption, blockIdOption)
  }

  /** Store the bytes of received data as a data block into Spark's memory. */
  def pushBytes(
      bytes: ByteBuffer,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ByteBufferBlock(bytes), metadataOption, blockIdOption)
  }

  /** Store block and report it to driver */
  def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    val blockId = blockIdOption.getOrElse(nextBlockId)
    val numRecords = receivedBlock match {
      case ArrayBufferBlock(arrayBuffer) => arrayBuffer.size
      case _ => -1
    }

    val time = System.currentTimeMillis
    // 使用ReceiverBlockManager,调用storeBlock方法存储block到BlockManager中
    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
    logDebug(s"Pushed block $blockId in ${(System.currentTimeMillis - time)} ms")

    // 封装一个ReceivedBlockInfo对象
    val blockInfo = ReceivedBlockInfo(streamId, numRecords, blockStoreResult)
    // 发送消息到ReceiverTracker中
    val future = trackerActor.ask(AddBlock(blockInfo))(askTimeout)
    Await.result(future, askTimeout)
    logDebug(s"Reported block $blockId")
  }

  /** Report error to the receiver tracker */
  def reportError(message: String, error: Throwable) {
    val errorString = Option(error).map(Throwables.getStackTraceAsString).getOrElse("")
    trackerActor ! ReportError(streamId, message, errorString)
    logWarning("Reported error " + message + " - " + error)
  }

  override protected def onStart() {
    /**
      * 启动blockGenerator线程。
      * 数据接收的时候,运行在worker端的executor端负责数据接收后的一些存取工作,
      * 以及配合ReceiverTracker工作。
      *
      * 因此,在executor上启动Receiver之前,首先会启动Receiver相关的blockGenerator
      */
    blockGenerator.start()
  }

  override protected def onStop(message: String, error: Option[Throwable]) {
    blockGenerator.stop()
    env.actorSystem.stop(actor)
  }


  override protected def onReceiverStart() {
    val msg = RegisterReceiver(
      streamId, receiver.getClass.getSimpleName, Utils.localHostName(), actor)
    val future = trackerActor.ask(msg)(askTimeout)
    Await.result(future, askTimeout)
  }

  override protected def onReceiverStop(message: String, error: Option[Throwable]) {
    logInfo("Deregistering receiver " + streamId)
    val errorString = error.map(Throwables.getStackTraceAsString).getOrElse("")
    val future = trackerActor.ask(
      DeregisterReceiver(streamId, message, errorString))(askTimeout)
    Await.result(future, askTimeout)
    logInfo("Stopped receiver " + streamId)
  }

  /** Generate new block ID */
  private def nextBlockId = StreamBlockId(streamId, newBlockId.getAndIncrement)

  private def cleanupOldBlocks(cleanupThreshTime: Time): Unit = {
    logDebug(s"Cleaning up blocks older then $cleanupThreshTime")
    receivedBlockHandler.cleanupOldBlocks(cleanupThreshTime.milliseconds)
  }
}
