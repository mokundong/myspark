import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by MKD on 2017/5/22.
  */
object JDBCDemo {
  def main(args: Array[String]): Unit = {
    //注册jdbc驱动
    Class.forName("com.mysql.jdbc.Driver")

    val prop = new Properties()
    prop.put("user","hive")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")

    val url = "jdbc:mysql://192.168.1.113:3306/big3"

    val table = "customers"
    //
    val sess = SparkSession.builder().master("local[4]").appName("jdbcdemo").getOrCreate()
    val df = sess.read.jdbc(url,table,prop)
    df.foreach(e=>{
      println(e.getInt(0) + "," + e.getString(1))
    })
    //写入数据到mysql
    df.filter("id < 3").write.jdbc(url,"test",prop)
  }
}
