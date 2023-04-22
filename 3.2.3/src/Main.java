
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.SerializableConfiguration;

import scala.Tuple2;
public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("");
        Configuration HBconf=HBaseConfiguration.create();
        HBconf.set("hbase.zookeeper.quorum", "cluster1");
        HBconf.set("hbase.zookeeper.property.clientPort", "2181");
        HBconf.set(TableInputFormat.INPUT_TABLE, "student");
        SerializableConfiguration SHBconf=new SerializableConfiguration(HBconf);
        // final HTable table=new HTable(HBconf, "student");
        try(JavaSparkContext jsc=new JavaSparkContext(conf)){
            JavaRDD<Result> rawdata=jsc.newAPIHadoopRDD(SHBconf.value(), TableInputFormat.class,
             ImmutableBytesWritable.class,
              Result.class).values();
            //System.out.println("\n***"+rawdata.count()+"***\n\n");
            JavaRDD<String> data=rawdata.map(
                new Function<Result,String>() {
                    public String call(Result v1) throws Exception {
                        //System.out.println("call() called with argument: " + v1.value().toString());
                        byte[] rowkey=v1.getRow();
                        byte[] c1=v1.getValue(Bytes.toBytes("score"), Bytes.toBytes("123001"));
                        byte[] c2=v1.getValue(Bytes.toBytes("score"), Bytes.toBytes("123002"));
                        byte[] c3=v1.getValue(Bytes.toBytes("score"), Bytes.toBytes("123003"));
                        return Bytes.toString(rowkey)+" "+Bytes.toString(c1)+" "+Bytes.toString(c2)+" "+Bytes.toString(c3);
                    };
                }
            );
            //System.out.println("\n***"+data.count()+"***\n\n");
            insertSum(data, SHBconf);
            insertAvg(data, SHBconf);
            System.out.println("***finish***");
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void insertSum(JavaRDD<String> data,final SerializableConfiguration SHBconf) throws IOException{
        JavaPairRDD<String,Integer> sum=data.flatMapToPair(
                // new PairFunction<String,String,Integer>() {
                //     public scala.Tuple2<String,Integer> call(String t) throws Exception {
                //         String[] res=t.split(" ");
                //         return new Tuple2<String,Integer>(res[0], Integer.parseInt(res[2]));
                //     };
                // }
                new PairFlatMapFunction<String,String,Integer>() {
                    public Iterable<Tuple2<String,Integer>> call(String t) throws Exception {
                        String[] tmp=t.split(" ");
                        ArrayList<Tuple2<String,Integer>> tuple2s=new ArrayList<Tuple2<String,Integer>>();
                        tuple2s.add(new Tuple2<String,Integer>(tmp[0],Integer.parseInt(tmp[1])));
                        tuple2s.add(new Tuple2<String,Integer>(tmp[0],Integer.parseInt(tmp[2])));
                        tuple2s.add(new Tuple2<String,Integer>(tmp[0],Integer.parseInt(tmp[3])));
                        //System.out.println(tuple2s);
                        return tuple2s;
                    }
                }
            ).reduceByKey(new Function2<Integer,Integer,Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1+v2;
                }
            });
        //System.out.println("\n*2**"+sum.count()+"***\n\n");
        sum.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                final HTable table=new HTable(SHBconf.value(), "student");
                String rowkey=t._1();
                Integer value=t._2();
                Put p=new Put(Bytes.toBytes(rowkey));
                //System.out.println("\n***"+rowkey+" "+value+" ***\n");
                p.add(Bytes.toBytes("stat_score"), Bytes.toBytes("sum"), Bytes.toBytes(value.toString()));
                table.put(p);
                table.close();
            }
        });
        
        return;
    }
    public static void insertAvg(JavaRDD<String>data,final SerializableConfiguration SHBconf)throws IOException{
        JavaPairRDD<String,Double> avg=data.flatMapToPair(
            new PairFlatMapFunction<String,String,Integer>() {
                public Iterable<Tuple2<String,Integer>> call(String t) throws Exception {
                    String[] tmp=t.split(" ");
                    ArrayList<Tuple2<String,Integer>> tuple2s=new ArrayList<Tuple2<String,Integer>>();
                    tuple2s.add(new Tuple2<String,Integer>(tmp[0],Integer.parseInt(tmp[1])));
                    tuple2s.add(new Tuple2<String,Integer>(tmp[0],Integer.parseInt(tmp[2])));
                    tuple2s.add(new Tuple2<String,Integer>(tmp[0],Integer.parseInt(tmp[3])));
                    //System.out.println(tuple2s);
                    return tuple2s;
                }
            }
        ).reduceByKey(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1+v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String,Integer>,String,Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<String,Double>(t._1(), t._2()/3.0);
            }
        });
        //System.out.println("\n***"+avg.count()+"***\n\n");
        avg.foreach(new VoidFunction<Tuple2<String,Double>>() {
            @Override
            public void call(Tuple2<String, Double> t) throws Exception {
                HTable table=new HTable(SHBconf.value(), "student");
                String rowkey=t._1();
                Double value=t._2();
                Put p=new Put(Bytes.toBytes(rowkey));
                p.add(Bytes.toBytes("stat_score"), Bytes.toBytes("avg"), Bytes.toBytes(value.toString()));
                table.put(p);
                table.close();
            }
        });
        return;
    }
}