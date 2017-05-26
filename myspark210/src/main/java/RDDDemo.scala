import java.io.FileWriter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RDDDemo{
  def main(args: Array[String]): Unit ={
    //创建SparkConf
    val conf = new SparkConf()
    conf.setAppName("rddDemo")
    //创建master,local[4]指定本地开启线程数
    conf.setMaster("local[4]")
    //conf.getConf.set("spark.memory.dffHeap.enabled","false")
    //创建sparkContext对象
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10)

    /*
    *储存
    rdd.persist(StorageLevel.DISK_ONLY_2)
    for (elem <- rdd.collect()){
      println(elem)
    }
    */
    /*
    val rdd2 = rdd.reduce((a,b)=>a + b)
    //mapPartitions
    val r1 = sc.makeRDD(1 to 10,4)

    def tt(a:Iterator[Int]):Iterator[Int] = {
      System.out.println("xxx")
      a
    }
    val r2 = r1.mapPartitions(tt)
    val r3 = r2.collect()
    r3.foreach(print(_))
    System.out.println("====================")
    r1.map(e=>{println("uuu");e}).collect
    */
    /*
    //mapPartitionWithIndex
    System.out.println("====================")
    def t2(index:Int,it:Iterator[Int]) : Iterator[Int]={
      val tname = Thread.currentThread().getName
      System.out.println(tname + ":" + index);
      it
    }
    r1.mapPartitionsWithIndex(t2).collect()
    */
    //采样
//    val r4 = sc.makeRDD(1 to 4,2)
//    r4.sample(false,0.5).collect().foreach(println)//0.5 采样比例（可换）
//    //union
//    r4.union(sc.makeRDD(10 to 15)).collect().foreach(println)
//    //去重
//    sc.makeRDD(Array(1,1,2,3,3,4,4,4,5)).distinct().collect().foreach(println)
//    //自定义分区函数
//    class MyPartitioner extends org.apache.spark.Partitioner{
//      def numPartitions:Int = 2
//      def getPartition(key:Any): Int = 0
//    }
//    val r5 = sc.makeRDD(Array((2,"tom"),(1,"tomas"),(3,"tomasLss")))
//    r5.repartitionAndSortWithinPartitions(new MyPartitioner).collect().foreach(println)
//    //聚合
//    val r6 = sc.makeRDD(Array((1,100),(2,200),(2,250),(3,300),(3,330)))
//    r6.persist(StorageLevel.OFF_HEAP)
//    r6.collect
//    r6.collect

    //广播变量
    val bc = sc.broadcast("hello world")
    //System.out.println(bc.value)
    rdd.map(e=>{
      val f = new FileWriter("/home/hadoop/t.txt",true)
      while(bc.value.equals("c")){
        f.write("hello world\r\n")
        Thread.sleep(1000)
      }
      e
    })
    //累加器
    val sum = sc.longAccumulator("sum")
    val rdd6 = sc.makeRDD(1 to 3)
    rdd6.map(e=>{
      println(sum.value)
      sum.add(e*3)
      e
    }).collect()
    println(sum.value)
  }
}
