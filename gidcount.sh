#!/bin/sh

WORK_DIR=/tmp
RES_DIR=$WORK_DIR/gidcountcsv

if [ -d $RES_DIR ]; then
    rm -fr $RES_DIR
fi

ROOT_DIR=/nexus/R
ACCESS_LOG_FILE=$ROOT_DIR/gitlab_access.log
if [ -f $ACCESS_LOG_FILE ]; then
#    rm -f $ACCESS_LOG_FILE
echo $ACCESS_LOG_FILE
fi
#scp -i $ROOT_DIR/gitlab.pem ec2-user@xxx.xxx.xxx.xxx:/var/log/gitlab/nginx/gitlab_access.log $ROOT_DIR

SPARK_HOME=/nexus/spark-3.2.1-bin-hadoop3.2
$SPARK_HOME/bin/spark-submit --class com.spark.example.app.GitLabUserLoginAnalytics --master local[2] /home/ec2-user/sparksimplejavapp-0.0.1-SNAPSHOT.jar $ACCESS_LOG_FILE $RES_DIR

cat $RES_DIR/part-* > $RES_DIR/gidcount.csv
#CSV_DATA_FILE=`ls -l $RES_DIR/*.txt | awk '{print $9}'`

#sed -i "s:(::; s:)::; " $RES_DIR/gidcount.csv
sed -i.bak "/^(-,/d; s:(::; s:)::; 1s/^/gid,count\n/;" $RES_DIR/gidcount.csv
cat $RES_DIR/gidcount.csv
#Rscript $ROOT_DIR/ipcountbarplot.R $CSV_DATA_FILE
