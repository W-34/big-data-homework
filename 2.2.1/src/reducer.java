import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class reducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
        Main.ans++;
        context.write(key, new Text(""));
    }
}
