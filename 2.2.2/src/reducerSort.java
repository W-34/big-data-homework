import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class reducerSort extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
    private static IntWritable num=new IntWritable(1);//rank
    public void reduce(IntWritable key,Iterable<IntWritable>values,Context context)throws IOException,InterruptedException{
        for(IntWritable i:values){//repeat k times which k=num of values
            context.write(num,key);
            num=new IntWritable(num.get()+1);//rank++
        }
    }
}
