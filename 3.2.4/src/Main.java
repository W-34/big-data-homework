
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.util.SerializableConfiguration;
public class Main {
    static SerializableConfiguration SHBconf;
    public static void main(String[] args) throws Exception {
        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("");
        Configuration HBconf=HBaseConfiguration.create();
        HBconf.set("hbase.zookeeper.quorum", "cluster1");
        HBconf.set("hbase.zookeeper.property.clientPort", "2181");
        //HBconf.set(TableInputFormat.INPUT_TABLE, "student");
        SHBconf=new SerializableConfiguration(HBconf);
        addRecord("student", "7777777", "score", "123001", "60");
        addRecord("student", "7777777", "score", "123002", "70");
        addRecord("student", "7777777", "score", "123003", "80");
        scanColumn("student", "score");
        scanColumn("student", "stat_score:sum");
        deleteRow("student", "7777777");
        return;
    }
    //3.2.4.1 添加数据
    public static void addRecord(String tableName, String row,
                                 String columnFamily, String column, String value) throws Exception {
        HTable table = new HTable(SHBconf.value(), tableName);
        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
            Bytes.toBytes(value));
        table.put(put);
        table.close();
    }
    //3.2.4.2 查询数据
    public static void scanColumn(String tableName,String column) throws Exception{
        HTable table = new HTable(SHBconf.value(), tableName);
        String[] str= column.split(":");
        if(str.length == 2) { // columnFamily:column
            ResultScanner scan = table.getScanner(str[0].getBytes(), str[1].getBytes());
            for (Result result : scan) {
                System.out.println(new String(result.getValue(str[0].getBytes(), str[1].getBytes())));
            }
            scan.close();
        }
        else{//columnFamily
            ResultScanner scan = table.getScanner(str[0].getBytes());
            for(Result result :scan){
                Map<byte[],byte[]> myMap = result.getFamilyMap(Bytes.toBytes(column));
                ArrayList<String> cols = new ArrayList<String>();
                for(Map.Entry<byte[],byte[]> entry:myMap.entrySet()){
                    cols.add(Bytes.toString(entry.getKey()));
                }
                for(String st :cols){
                    System.out.print(st+ " : "+ new String(result.getValue(column.getBytes(),st.getBytes()))+"    ");
                }
                System.out.println();
            }
            scan.close();
        }
    }
    //3.2.4.3 删除数据
    public static void deleteRow(String tableName, String row) throws Exception {
        HTable table = new HTable(SHBconf.value(), tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }
}