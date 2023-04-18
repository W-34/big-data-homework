import java.io.StringReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
object lab3 {
    def main(args: Array[String]) {
        val inputFile =  "/home/hadoop/data6.txt"
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
	//conf.set("spark.testing.memory", "500000000")
        val sc = new SparkContext(conf)
                val textFile = sc.textFile(inputFile)
                val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
                wordCount.foreach(println)
		wordCount.saveAsTextFile("/home/hadoop/output6")    
    }
}
