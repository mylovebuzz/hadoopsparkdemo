package com.spark.example.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class GitLabUserLoginAnalytics {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("GitLabUserLoginAnalytics").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rawContent = sc.textFile(args[0]);
		JavaRDD<String> gids= rawContent.map(record -> record.split(" ")[2]);
        JavaPairRDD<String, Integer> gidCountPair = gids.mapToPair(
        		(PairFunction<String, String, Integer>) gid -> new Tuple2<>(gid, 1));
        JavaPairRDD<String, Integer> gitSumRdd = gidCountPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer cnt1, Integer cnt2) throws Exception {
        		
        	return cnt1 + cnt2;
        }
        });
        gitSumRdd.saveAsTextFile(args[1]);
		sc.close();
	}
}
