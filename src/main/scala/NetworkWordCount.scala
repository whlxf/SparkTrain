import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
  * SparkStreaming操作socket数据（TCP source） socketTextStream
  * 测试方式：命令行 nc -lk 9999  输入要发送的数据
  */

object NetworkWordCount {
  /**
    * 实现步骤：
    * 1.创建ssc:两个参数 conf Seconds()
    * 2.ssc.socketTextStream()接收数据,两个参数 hostname port
    * 3.操作数据 transform
    * 4.output 数据
    * 5.启动ssc
    * 6.设置停止方式
    */

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("localhost",9999)

    val results = lines.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_)

    results.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
