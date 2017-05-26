import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Created by MKD on 2017/5/3.
 */
public class WordCountJava8 {
    public static void main(String[] args){
        //创建SparkConf对象
        SparkConf conf = new SparkConf();
        conf.setAppName("word count java");
        conf.setMaster("local[4]");
        //创建JAVASPARKContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("d:/work/a.txt");
        //压扁的java版
        JavaRDD<Integer> rdd2 = rdd.map(s -> s.length());
        Integer len = rdd2.reduce((a,b) -> a + b);
        System.out.println(len);
    }
}
