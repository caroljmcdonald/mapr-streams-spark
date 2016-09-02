package com.sparkkafka.example

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ConsumerDataLossTest {

  def updateFunction(newValues: Seq[Long], runningCount: Option[Long]): Option[Long] = {
    val newCount = newValues.sum + runningCount.getOrElse(0L)
    Some(newCount)
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: ConsumerDataLossTest <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("ConsumerDataLossTest")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(".")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> "groupID",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> "1000")
    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2).map(x => (x, 1L))
    val runningCounts = lines.updateStateByKey[Long](updateFunction _)

    runningCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
