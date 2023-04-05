import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class mapperSort extends Mapper<Object,Text,IntWritable,IntWritable>{
    private static IntWritable data=new IntWritable();
    public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
        data.set(Integer.parseInt(value.toString()));
        context.write(data,new IntWritable(1));//<int,1>
    }
}
