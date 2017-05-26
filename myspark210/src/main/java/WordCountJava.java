import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by MKD on 2017/5/2.
 * Java实现WordCount
 */
public class WordCountJava {
    public static void main(String[] args){
        //创建SparkConf对象
        SparkConf conf = new SparkConf();
        conf.setAppName("word count java");
        conf.setMaster("local[4]");
        //创建JAVASPARKContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("d:/work/a.txt");
        //压扁的java版
        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception{
                List<String> list = new ArrayList<String>();
                for (String str: s.split(" ")){
                    list.add(str);
                }
                return list.iterator();
            }
        });

        JavaPairRDD<String,Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>(){
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        //
        JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        List<Tuple2<String,Integer>> list = rdd4.collect();
        for(Tuple2<String,Integer> t : list){
            System.out.println(t._1() + "=" + t._2());
        }
    }
}
