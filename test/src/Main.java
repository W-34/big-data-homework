import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
public class Main {
    public static void main(String[] args) throws Exception {
        String s0=args[0],s1=args[1];
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"w34");
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path(s0));
        FileOutputFormat.setOutputPath(job,new Path(s1));
        job.setMapOutputKeyClass(Integer.class);
        job.setMapOutputValueClass(Integer.class);
        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(Integer.class);
        boolean res=job.waitForCompletion(true);
        System.out.println(res);
    }
}
