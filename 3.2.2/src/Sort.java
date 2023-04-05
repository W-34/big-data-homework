import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
public class Sort {
    public static int ans=0;
    public static void main(String[] args) throws Exception {
        String s0="./input2",s1="./output2";
        Path path=new Path(s1);
        Configuration conf=new Configuration();
        FileSystem fileSystem=path.getFileSystem(conf);
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }
        Job job=Job.getInstance(conf,"Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(mapperSort.class);
        job.setReducerClass(reducerSort.class);
        //job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path(s0));
        FileOutputFormat.setOutputPath(job,new Path(s1));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        boolean res=job.waitForCompletion(true);
        System.out.println(res);
        System.out.println(ans+" numbers are sorted");
    }
}
class Partition extends Partitioner<IntWritable, IntWritable> {
    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        int MaxNumber = 1024;
        int bound = MaxNumber / numPartitions + 1;
        int keynumber = key.get();
        for (int i = 0; i < numPartitions; i++) {
            if (keynumber < bound * i && keynumber >= bound * (i - 1))
                return i - 1;
        }
        return 0;
    }
}
