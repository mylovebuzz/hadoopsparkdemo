package com.spark.example.app

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{RowFactory, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object GitLabUserActionAnalytics extends Serializable {

 // NG: should extends on object just like above
 // object FieldTool {
//    def createField(fieldName:String): StructField = {
//
//      StructField(fieldName, StringType, nullable = true)
//    }
 // }

  def main(args: Array[String]): Unit = {
// https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/
    val conf = new SparkConf().setAppName("GitLabUserActionAnalytics").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().appName("GitLabUserActionAnalytics").getOrCreate()
    val rawContent = sc.textFile(args(0))
    val schemaString = "gid action"
    //val schema = StructType(schemaString.split(" ").map(fieldName => createField(fieldName)))
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true)))
    /* OK: java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Ljava/lang/Object scalac version incompatible
    val schema = StructType(schemaString.split(" ").map(createField))
    val schema = StructType(schemaString.split(" ").toArray[String].map(field => StructField(field, StringType, nullable = true)))
    check $SPARK_HOME/jars/scala* for scala version */
    /* OK
    val fields = Array(StructField("gid", StringType, nullable = true), StructField("action", StringType, nullable = true))
    val schema = StructType(fields)

    val schemaString = "gid action"
    val arrayBuf = ArrayBuffer[StructField]()
    schemaString.split(" ").map { field => arrayBuf += StructField(field, StringType, nullable = true) }
    //https://wenku.baidu.com/view/c0e2f30a7075a417866fb84ae45c3b3567ecddaa.html
    val schema = StructType(arrayBuf.toArray)

    val fields = schemaString.split(" ").map(field => StructField(field, StringType, nullable = true))
    val schema = StructType(fields)
    */
//    object Line2RDD {
//      def readLine2RDD(record: String): Row = {
//        val gid = record.split(" ")(2)
//        val action = record.split("\"")(1)
//        RowFactory.create(gid, action)
//      }
//    }
//    val rowRDD = rawContent.map(Line2RDD.readLine2RDD)
    val rowRDD = rawContent.map(record => RowFactory.create(record.split(" ")(2), record.split("\"")(1)))
    val dfGidAction = ss.createDataFrame(rowRDD, schema)
    dfGidAction.createTempView("gidaction")
    val rowGitAction = ss.sql("select gid, action from gidaction where gid <> '-' order by gid, action")
    rowGitAction.write.option("header", value = true).csv(args(1))
    ss.close()
  }
}
