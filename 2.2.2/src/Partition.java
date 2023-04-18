import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
public class Partition extends Partitioner<IntWritable, IntWritable> {
    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        int MaxNumber = 65536;
        int bound = MaxNumber / numPartitions + 1;
        int keynumber = key.get();
        for (int i = 0; i < numPartitions; i++) {
            if (keynumber < bound * i && keynumber >= bound * (i - 1))
                return i - 1;
        }
        return 0;
    }
}