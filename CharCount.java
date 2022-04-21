package com.spark.example.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class CharCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		JavaRDD<String> textFile = sc.textFile("hdfs://...");
//		JavaPairRDD<String, Integer> counts = textFile
//		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//		    .mapToPair(word -> new Tuple2<>(word, 1))
//		    .reduceByKey((a, b) -> a + b);
//		counts.saveAsTextFile("hdfs://...");

		JavaRDD<String> lines = sc.textFile("/tmp/gitlab_access.log");
		//JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String s) throws Exception {
				System.out.println("wangxian debug param: " + s);
				int len = s.length();
				System.out.println("wangxian debug param length: " + len);
				return len;
			}
		});

		int totalLength = lineLengths.reduce((a, b) -> a + b);
		System.out.println("totle length is: " + totalLength);
		System.out.println(String.valueOf(lineLengths.collect()));
		lines.saveAsTextFile("/tmp/countresult");
		sc.close();
	}
}
