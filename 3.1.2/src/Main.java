import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("");
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            JavaRDD<String> distdata=jsc.textFile("/home/hadoop/data6.txt");
            JavaRDD<String> words=distdata.flatMap(new FlatMapFunction<String,String>() {
                @Override
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            });
            JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<>(s, 1);
                }
            });
            JavaPairRDD<String, Integer> ans = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
            ans.saveAsTextFile("./output6");
        }
    }
}