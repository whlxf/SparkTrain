import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming 处理文件系统的数据 textFileStream
  */

object FileWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(conf,Seconds(5))
    //moving 新增加的文件 到 监控目录下，必须是moving整体移动的方式
    val lines = ssc.textFileStream("hdfs://master:9000/streaming/ss/")
    val results = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
