import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by MKD on 2017/5/2.
  */
object WordCountSub {
  def main(args: Array[String]): Unit ={
    //创建配置对象
    val conf = new SparkConf()
    conf.setAppName("word count")
    conf.setMaster("local[4]")
    //创建上下文对象
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("hdfs://192.168.64.100:8020/user/hadoop/words.txt")
    val rdd2 = rdd1.flatMap(line=>line.split(" "))
    val rdd3 = rdd2.map(w=>{
      (w,1)
    })
    val rdd4 = rdd3.reduceByKey(func = (a, b) => {
      a + b
    })
    rdd4.collect()
    System.out.println("over")
  }
}
