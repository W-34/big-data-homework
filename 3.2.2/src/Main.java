
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
public class Main {
    public static void main(String[] args) throws Exception {
        try{
            String tableName="student";
            String[] columns={"information","score","stat_score"};
            Configuration conf=HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            HTable table=new HTable(conf,tableName);
            insertInformation(table);
            insertScore(table);
            table.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void insertInformation(HTable table) throws IOException{
        List<String> list=Files.readAllLines(Paths.get("/home/hadoop/data7.txt"),StandardCharsets.UTF_8);
        for(String line :list){
            String[] data=line.split(" ");
            Put p=new Put(Bytes.toBytes(data[0]));
            p.add(Bytes.toBytes("information"), Bytes.toBytes("name"), Bytes.toBytes(data[1]));
            p.add(Bytes.toBytes("information"), Bytes.toBytes("sex"), Bytes.toBytes(data[2]));
            p.add(Bytes.toBytes("information"), Bytes.toBytes("age"), Bytes.toBytes(data[3]));
            table.put(p);
        }
        return;
    }
    public static void insertScore(HTable table)throws IOException{
        List<String> list=Files.readAllLines(Paths.get("/home/hadoop/data8.txt"),StandardCharsets.UTF_8);
        for(String line :list){
            String[] data=line.split(" ");
            Put p=new Put(Bytes.toBytes(data[0]));
            p.add(Bytes.toBytes("score"), Bytes.toBytes(data[1]), Bytes.toBytes(data[2]));
            table.put(p);
        }
        return;
    }
}