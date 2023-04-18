cd ~
scalac -cp /usr/local/spark-1.6.3-bin-hadoop2.6/lib/*: lab3.scala
java -cp /usr/local/hadoop-2.6.5/share/hadoop/common/*:/usr/local/hadoop-2.6.5/share/hadoop/common/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/hdfs/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/*:/usr/local/hadoop-2.6.5/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/lib/*:/usr/local/hadoop-2.6.5/share/hadoop/yarn/*:/usr/local/scala-2.10.6/lib/*:/usr/local/spark-1.6.3-bin-hadoop2.6/lib/*: lab3
rm -f ./*.class