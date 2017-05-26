import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by MKD on 2017/5/21.
  */
object SparkSQLDemo {
  case class User(id:Int,name:String,age:Int)   //必须放到成员变量位置
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("sqlDemo")
    val sc = new SparkContext(conf)

    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val rdd = sc.parallelize(Array("1,tom,12","2,tomas,13","3,tomasLee,14"))

    def toUser(ss:Seq[String]):User = User(ss(0).toInt,ss(1),ss(2).toInt)

    val rdd2 = rdd.map(_.split(",")).map(toUser(_))
    val dff = rdd2.toDF("id","name","age")
    dff.foreach(e=>{
        println(e.getInt(0) + ","+ e.getString(1) + ","+ e.getInt(2))
    })
  }
}
