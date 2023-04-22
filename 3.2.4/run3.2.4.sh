cd ~
javac -cp /usr/local/scala-2.10.6/lib/*:/usr/local/spark-1.6.3-bin-hadoop2.6/lib/*:/usr/local/hbase-1.2.6/lib/*: Main.java
java -cp /usr/local/scala-2.10.6/lib/*:/usr/local/spark-1.6.3-bin-hadoop2.6/lib/*:/usr/local/hbase-1.2.6/lib/*: Main
rm -f ./*.class