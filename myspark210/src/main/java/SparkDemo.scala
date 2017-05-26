import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo{
  def main(args: Array[String]): Unit ={
    val file = "d:/work/a.txt"
    //创建SparkConf
    val conf = new SparkConf().setAppName("simple Application")
    //创建master,local[4]指定本地开启线程数
    conf.setMaster("local[4]")
    //创建sparkContext对象
    val sc = new SparkContext(conf)
    val len = sc.textFile(file).collect().length
    println(len)
  }
}
