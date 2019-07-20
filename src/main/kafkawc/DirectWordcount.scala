/*
*spark Streaming 对接kafka基于direct方式
* */
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object DirectWordcount {
  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("Usage:DirectWordcount <broker> <topics>")
      System.exit(1)
    }
    val Array(brokers, topics) =args
    val sparkConf = new SparkConf().setAppName("kafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(10))
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"->brokers)
    //spark Streaming 对接kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)
      //createStream(ssc,zkQuorum,group,topicMap)
    // val messgaes = KafkaUtils.createDirectStream(ssc,zkQuorum,groupId,topicMap)
    //自己测试为啥取第二个
    print("开始统计了*****")
    messages.print()
    messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
