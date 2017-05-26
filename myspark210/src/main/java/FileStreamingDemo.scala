import com.mkd.stream.SparkStreamDemo.getClass
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by MKD on 2017/5/24.
  */
object FileStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val stream = ssc.fileStream("d:/data")
    stream.print()
    ssc.start()
    ssc.stop()
    ssc.awaitTermination()

  }

}
