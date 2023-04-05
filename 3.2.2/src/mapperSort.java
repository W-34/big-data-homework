import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class mapperSort extends Mapper<Object,Text,Text,Text>{
    private static Text text=new Text();
    public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
        text=value;
        context.write(text,new Text(""));
    }
}
