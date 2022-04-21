# hadoopsparkdemo
# how to run CharCount in spark:
# enter spark home directory
cd spark-3.2.1-bin-hadoop3.2/
# run jar file
./bin/spark-submit --class com.spark.example.app.CharCount --master local[2] /home/ec2-user/sparksimplejavapp-0.0.1-SNAPSHOT.jar
