import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
public class Main {
    public static void main(String[] args) throws Exception {
        String s0="./input",s1="./output";
        Path path=new Path(s1);
        Configuration conf=new Configuration();
        FileSystem fileSystem=path.getFileSystem(conf);
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }
        Job job=Job.getInstance(conf,"Main");
        job.setJarByClass(Main.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path(s0));
        FileOutputFormat.setOutputPath(job,new Path(s1));
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        boolean res=job.waitForCompletion(true);
        System.out.println(res);
    }
}