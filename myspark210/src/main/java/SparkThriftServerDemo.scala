import java.sql.DriverManager

/**
  * Created by MKD on 2017/5/23.
  */
object SparkThriftServerDemo {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.64.100:10000/hive1")
    val st = conn.createStatement();
    val rs = st.executeQuery("select count(1) from test5")
    while (rs.next())
      System.out.println(rs.getInt(1))
    System.out.println("over")
  }
}
