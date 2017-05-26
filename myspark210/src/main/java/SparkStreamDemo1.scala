
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
object SparkStreamDemo1{
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setAppName("NetworkWorkCount").setMaster("local[2]")
//    val conf = new SparkConf().setAppName("NetworkWorkCount").setMaster("spark://192.168.64.100:7077")
    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.socketTextStream("localhost",9999)
//    val lines = ssc.socketTextStream("192.168.64.100",9999) //打包
    ssc.checkpoint("d:\\tmp")
//    ssc.checkpoint("/home/hadoop/tmp")  //打包
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.saveAsTextFiles("d:\\tmp\\a")
    ssc.start()
    //ssc.awaitTermination()
    Thread.sleep(60000) //运行1分钟
    ssc.stop()
  }
}

