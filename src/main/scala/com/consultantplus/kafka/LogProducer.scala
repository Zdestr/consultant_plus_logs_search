package com.consultantplus.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

object LogProducer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LogProducer <logs-directory> <bootstrap-servers>")
      sys.exit(1)
    }

    val logsDir    = args(0)
    val brokers    = args(1)
    val topic      = sys.env.getOrElse("KAFKA_TOPIC", "raw-session-logs")

    val props = new Properties()
    props.put("bootstrap.servers",       brokers)
    props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("acks",             "all")
    props.put("retries",          "3")
    props.put("enable.idempotence", "true")
    props.put("linger.ms",        "5")

    val producer = new KafkaProducer[String, Array[Byte]](props)

    val logFiles = Files.list(Paths.get(logsDir)).iterator().asScala
      .filter(p => p.toFile.isFile)
      .toSeq

    logFiles.foreach { path =>
      val sessionId = path.getFileName.toString.stripSuffix(".log")
      val content   = Files.readAllBytes(path)
      val record    = new ProducerRecord[String, Array[Byte]](topic, sessionId, content)
      producer.send(record)
    }

    producer.flush()
    producer.close()
    println(s"[LogProducer] Sent ${logFiles.size} files to topic '$topic'.")
  }
}
