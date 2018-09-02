import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
  * 带有状态的算子 UpdateStateByKey：
  * 为SparkStreaming中的每一份batch的key维护一个state，通过更新函数，对这个key的值不断更新
  * 1）定义状态：可以是任意数据类型；
  * 2）定义状态更新函数：用一个函数指定怎样使用以前的数据
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.checkpoint("/data/stream/")

    val lines = ssc.socketTextStream("localhost",9999)

    val result = lines.flatMap(_.split(" ")).map((_,1))
    //val state = result.u

    //state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /*
  * @Description: 拿当前的数据去更新已有的或者以前的老数据
  */
  def updateFunc(currentValues:Seq[Int], preValues:Option[Int]):Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

}
