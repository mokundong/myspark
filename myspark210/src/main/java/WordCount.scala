import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MKD on 2017/5/2.
  */
object WordCount {
  def main(args: Array[String]): Unit ={
    //创建配置对象
    val conf = new SparkConf()
//    conf.setAppName("word count")
//    conf.setMaster("spark://192.168.64.100:7077")

    conf.setAppName("word count")
    //conf.setMaster("local[4]")

    //创建上下文对象
    val sc = new SparkContext(conf)
    /*
    val r = sc.textFile("d:/work/a.txt").flatMap(_.split(" ")).map(w => (w,1)).reduceByKey((a,b) => a + b).collect()
    r.foreach(line=>println(line._1 + "=" + line._2))
    */
    /**
      */
    //val r = sc.textFile("d:/work/a.txt",sc.defaultMinPartitions)
    val r = sc.textFile("hdfs://192.168.64.100:8020/user/hadoop/words.txt",sc.defaultMinPartitions)
        .flatMap(line => line.split(" "))
        .map(w => {
            (w,1)
        })
        .reduceByKey((a,b) => {
          println("xxxx")
          a + b;
        })
        .collect()
        .foreach({
          println(_)
        })

  }
}
