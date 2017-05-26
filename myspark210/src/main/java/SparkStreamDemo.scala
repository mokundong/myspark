

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
object SparkStreamDemo{
  def main(args: Array[String]):Unit = {
    System.out.println("开始流处理")
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 设置保存点，用于故障恢复
    ssc.checkpoint("d:\\tmp")
    println("Stream processing logic start")
    //创建离散流
    val stream = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER)
    //过滤包含ERROR字样的文本
    stream.filter(line => {println(line);line.contains("ERROR")})
    //打印流
    stream.print()
    //窗口化操作
    //errorLines.countByWindow(Seconds(30), Seconds(5)).print()
    println("Stream processing logic end")
    //启动流计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
}

