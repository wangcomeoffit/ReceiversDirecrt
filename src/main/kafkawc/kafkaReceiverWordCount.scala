import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/*
*spark Streaming 对接kafka基于receivers方式
* */
object kafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length != 4){
      System.err.println("Usage:ImoocStatStreamingAPP <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) =args
    val sparkConf = new SparkConf().setAppName("kafkaReceiverWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(30))
//    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("Error")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    //spark Streaming 对接kafka
    val messgaes = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

    // val messgaes = KafkaUtils.createDirectStream(ssc,zkQuorum,groupId,topicMap)

    //自己测试为啥取第二个
    print("开始统计了*****")
    //messgaes.print()
    messgaes.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
