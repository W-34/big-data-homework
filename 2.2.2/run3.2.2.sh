cd ~
mkdir input2
mkdir output2
hadoop fs -get /user/hadoop/data/附录实验数据三.txt ./input/data3.txt
hadoop fs -get /user/hadoop/data/附录实验数据四.txt ./input/data4.txt
javac -cp /usr/local/hadoop-2.6.5/share/hadoop/common/*:/usr/local/hadoop-2.6.5/share/hadoop/common/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/*: Sort.java mapperSort.java reducerSort.java Partition.java
jar cf Sort.jar Sort.class mapperSort.class reducerSort.class Partition.class
hadoop fs -put Sort.jar /user/hadoop/
hadoop jar Sort.jar Sort
rm -f ./*.class