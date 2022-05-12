package com.spark.example.app;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class NginxAccessLogAnalytics {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("NginxAccessLogAnalytics").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("SparkSession ").getOrCreate();

		JavaRDD<String> rawContent = sc.textFile(args[0]);
		String schemaString = "IP Status";
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, false);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = rawContent.map((Function<String, Row>) record -> {
			String[] attrIP = record.split(" ");
			String[] attrStatus = record.split("\" ");
            String[] attrStatusOnly = attrStatus[1].split(" ");
			return RowFactory.create(attrIP[0], attrStatusOnly[0]);
		});
		Dataset<Row> logDataFrame = spark.createDataFrame(rowRDD, schema);
		try {
			logDataFrame.createTempView("nginxlog");
            Dataset<Row> results = spark.sql("select IP from nginxlog");
            Dataset<String> ipDS = results.map(
            (MapFunction<Row, String>) row -> "IP: " + row.getString(0), Encoders.STRING());
            ipDS.show();
       
            Dataset<Row> records = spark.sql("select IP, Status from nginxlog order by IP");
            records.show();
            
            Dataset<Row> ipCountDS = spark.sql("select IP, count(*) as Count from nginxlog group by IP");
            ipCountDS.show();
            ipCountDS.write().option("header", true).csv(args[1]);
		} catch (AnalysisException e) {

			e.printStackTrace();
		}
		JavaRDD<List<String>> content = rawContent.map(new Function<String, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(String s) throws Exception {
				System.out.println("wangxian debug param: " + s);
				List<String> al = new ArrayList<String>();
				String strIp = s.split(" - ")[0];
				al.add(strIp);
				al.add(String.valueOf(Pattern.matches(s, ".+ 404 .+")));
				al.add(String.valueOf(Pattern.matches(s, ".+ 403 .+")));
				return al;
			}
		});
		//Map<String, Long> uniqueIpCount = rawContent.countByValue();
        //System.out.println(content);
		//rawContent.saveAsTextFile("/tmp/countresult");
		sc.close();
	}
}
