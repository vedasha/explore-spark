root      1296  1.0  4.8 2916224 190484 ?      Sl   08:22   0:14 
	java -Dspark.executor.memory=1g -Dspark.cores.max=1 -Dfile.encoding=UTF-8 -Xms1024m -Xmx1024m -XX:MaxPermSize=512m -Dzeppelin.log.file=/home/cloudera/zeppelin-0.5.6-incubating-bin-all/logs/zeppelin--quickstart.cloudera.log -cp ::/home/cloudera/zeppelin-0.5.6-incubating-bin-all/lib/*:/home/cloudera/zeppelin-0.5.6-incubating-bin-all/*::/home/cloudera/zeppelin-0.5.6-incubating-bin-all/conf 
	org.apache.zeppelin.server.ZeppelinServer

root      6076  0.0  0.0 108200   356 ?        S    08:29   0:00  
	\_ /bin/bash /home/cloudera/zeppelin-0.5.6-incubating-bin-all/bin/interpreter.sh -d 
	/home/cloudera/zeppelin-0.5.6-incubating-bin-all/interpreter/spark -p 37848

root      6089 10.9 22.8 2720084 896156 ?      Sl   08:29   0:57      
	\_ /usr/java/jdk1.7.0_67-cloudera/jre/bin/java -cp 
		/home/cloudera/zeppelin-0.5.6-incubating-bin-all/interpreter/spark/zeppelin-spark-0.5.6-incubating.jar:
		/usr/lib/spark/conf/:
		/usr/lib/spark/lib/spark-assembly-1.5.0-cdh5.5.0-hadoop2.6.0-cdh5.5.0.jar:
		/usr/lib/hadoop/etc/hadoop/:
		/usr/lib/spark/lib/spark-assembly.jar:
		/usr/lib/hadoop/lib/*:
		/usr/lib/hadoop/*:
		/usr/lib/hadoop-hdfs/lib/*:
		/usr/lib/hadoop-hdfs/*:
		/usr/lib/hadoop-mapreduce/lib/*:
		/usr/lib/hadoop-mapreduce/*:
		/usr/lib/hadoop-yarn/lib/*:
		/usr/lib/hadoop-yarn/*:
		/usr/lib/hive/lib/*:
		/usr/lib/flume-ng/lib/*:
		/usr/lib/paquet/lib/*:
		/usr/lib/avro/lib/* 
		-Xms1g -Xmx1g 
		-Dspark.executor.memory=1g -Dspark.cores.max=1 
		-Dfile.encoding=UTF-8 -Dfile.encoding=UTF-8 -Dzeppelin.log.file=/home/cloudera/zeppelin-0.5.6-incubating-bin-all/logs/zeppelin-interpreter-spark--quickstart.cloudera.log 
		-XX:MaxPermSize=256m 
		org.apache.spark.deploy.SparkSubmit --conf spark.driver.extraClassPath=::/home/cloudera/zeppelin-0.5.6-incubating-bin-all/interpreter/spark/zeppelin-spark-0.5.6-incubating.jar --conf spark.driver.extraJavaOptions= -Dspark.executor.memory=1g -Dspark.cores.max=1 -Dfile.encoding=UTF-8 -Dspark.executor.memory=1g -Dspark.cores.max=1 -Dfile.encoding=UTF-8 -Dzeppelin.log.file=/home/cloudera/zeppelin-0.5.6-incubating-bin-all/logs/zeppelin-interpreter-spark--quickstart.cloudera.log 
		--class org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer 
		--executor-memory 1G 
	/home/cloudera/zeppelin-0.5.6-incubating-bin-all/interpreter/spark/zeppelin-spark-0.5.6-incubating.jar 37848
