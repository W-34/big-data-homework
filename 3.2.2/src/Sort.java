import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        //job.setJarByClass(Sort.class);
        job.setJar("Sort.jar");
        job.setMapperClass(mapperSort.class);
        job.setPartitionerClass(Partition.class);
        job.setReducerClass(reducerSort.class);
        //job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path(s0));
        FileOutputFormat.setOutputPath(job,new Path(s1));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        boolean res=job.waitForCompletion(true);
        System.out.println(res);
    }
}
