package com.sparkkafka.example

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD

/*
 http://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-2-direct-approach-no-receivers
 MapR Streams Spark Streaming documentation
 http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
  http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Produce.html
*/
object SparkKafkaConsumer {

  case class CallDataRecord(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  // function to parse input into CDR class  
  def parseCallDataRecord(str: String): CallDataRecord = {
    val c = str.split("\\t", -1).map(str => (if (str.isEmpty()) "0" else str))
    CallDataRecord(c(0).toInt, c(1).toLong, c(2).toInt, c(3).toFloat,
      c(4).toFloat, c(5).toFloat, c(6).toFloat, c(7).toFloat)
  }

  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println("Usage: SparkKafkaConsumerDemo <brokers> <topic consume> <topic produce>.")
      System.exit(1)
    }
    val groupId = "testgroup"
    val offsetReset = "earliest"
    val pollTimeout = "1000"
    val Array(brokers, topicc, topicp) = args

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumer.getClass.getName)
      .set("spark.cores.max", "1")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("~/tmp")

    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )
    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList
    )

    val messagesDStream: InputDStream[(String, String)] = {
      KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)
    }

    val valuesDStream: DStream[String] = messagesDStream.map(_._2)

    valuesDStream.foreachRDD { rdd =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._

        val cdrDF = rdd.map(parseCallDataRecord).toDF()
        // Display the top 20 rows of DataFrame
        println("CDR data")
        cdrDF.show()
      
        val sRDD: RDD[String] = cdrDF.groupBy("squareId").count().orderBy(desc("count")).map(row => s"squareId: ${row(0)}, count: ${row(1)}")
        println("sending messages")
        
        sRDD.take(3).foreach(println)
        
        sRDD.sendToKafka[StringSerializer](topicp, producerConf)

      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
