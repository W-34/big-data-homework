cd ~
mkdir input
mkdir output
hadoop fs -get /user/hadoop/data/附录实验数据一.csv ./input/data1.csv
hadoop fs -get /user/hadoop/data/附录实验数据二.csv ./input/data2.csv
javac -cp /usr/local/hadoop-2.6.5/share/hadoop/common/*:/usr/local/hadoop-2.6.5/share/hadoop/common/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/*: Main.java mapper.java reducer.java
java -cp /usr/local/hadoop-2.6.5/share/hadoop/common/*:/usr/local/hadoop-2.6.5/share/hadoop/common/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/*: Main
rm -f ./*.class