package com.dq.check.service

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkContext

import scala.io.Source

object DataQualityFactoryPattern {

  //formatType, spark, ip_file, ipSchemaPath, dq_IpHiveQuery, val_sep, val_header, val_infr_schema, val_null_vl, val_charset)


  def determineFileFormat(fileFormat: String, spark:SparkSession, sc:SparkContext, dq_ipPath:String, schemaPath: String ,HiveTableName: String, val_sep: String, val_header: String, val_infr_schema: String, val_null_vl: String, val_charset: String, multi_line_flg: String, Dataset_Multi_Line_quote: String, Dataset_Multi_Line_escape: String):DataFrame = {
    fileFormat match {
      case "parquet" => processParquet(spark, dq_ipPath)
      case "hive" => processHive(spark, HiveTableName, sc)
      case "csv" => processCsv(spark, dq_ipPath, schemaPath, val_sep, val_header, val_infr_schema, val_null_vl, val_charset, multi_line_flg, Dataset_Multi_Line_quote, Dataset_Multi_Line_escape)
      case "json" => processJson(spark, dq_ipPath, schemaPath, multi_line_flg)
    }
 }


  def processParquet(spark: SparkSession, dq_IpPath:String ):DataFrame ={
    spark.read.parquet(dq_IpPath)
  }


  def processHive(spark: SparkSession,  HiveTableName: String,  sc:SparkContext):DataFrame ={
    //---Read user provided SQL
    val sparkHive = SparkSession.builder.appName("hive-load").getOrCreate()
    println("-------------------Hive query----------------------")

    spark.sql(s"select * from $HiveTableName")
 }


  def processCsv(spark: SparkSession, dq_CsvIpPath:String, schemaPath: String,val_sep: String, val_header: String, val_infr_schema: String, val_null_vl: String, val_charset: String, multi_line_flg: String, Dataset_Multi_Line_quote: String, Dataset_Multi_Line_escape: String ):DataFrame ={
    /*
    val val_sep = config.getString("Input_data.val_sep")
    val val_header = config.getString("Input_data.val_header")
    var val_infr_schema = config.getString("Input_data.val_infr_schema")
    val val_null_vl = config.getString("Input_data.val_null_vl")
    val val_charset = config.getString("Input_data.val_charset")
     */
    var Dataset_Multi_Line_quote_actual = ""
    if(Dataset_Multi_Line_quote != ""){
      Dataset_Multi_Line_quote_actual = Dataset_Multi_Line_quote
    }else{
      Dataset_Multi_Line_quote_actual = '"'.toString
    }

    var Dataset_Multi_Line_escape_actual = ""
    if(Dataset_Multi_Line_escape != ""){
      Dataset_Multi_Line_escape_actual = Dataset_Multi_Line_escape
    }else{
      Dataset_Multi_Line_escape_actual = '\\'.toString
    }

    if (val_infr_schema == "custom"){
      //---Read schema as json (user defined schema)
      //val readUserSchemaCsv = spark.sparkContext.textFile(schemaPath)
      //val consolidatedSchemaCsv = readUserSchemaCsv.collect().mkString
      val filename=schemaPath.split("/").last
      val consolidatedSchemaCsv = Source.fromFile(filename).getLines.mkString
      val schemaFromJson = DataType.fromJson(consolidatedSchemaCsv).asInstanceOf[StructType]

      println("processCsv -- custom schema: " + "header " + val_header + "multiLine " + multi_line_flg)
      spark.read.format("csv")
        .option("header", val_header)
        .option("sep", val_sep)
        .option("nullValue", val_null_vl)
        .option("charset",val_charset)
        .option("multiLine",multi_line_flg)
        .option("quote",Dataset_Multi_Line_quote_actual)
        .option("escape",Dataset_Multi_Line_escape_actual)
        .schema(schemaFromJson)
        .load(dq_CsvIpPath)
    }
    else{
      println("processCsv -- default schema: " + "header " + val_header + "multiLine " + multi_line_flg)
      spark.read.format("csv")
        .option("header", val_header)
        .option("inferSchema",val_infr_schema)
        .option("sep", val_sep)
        .option("nullValue", val_null_vl)
        .option("charset",val_charset)
        .option("multiLine",multi_line_flg)
        .option("quote",Dataset_Multi_Line_quote_actual)
        .option("escape",Dataset_Multi_Line_escape_actual)
        .load(dq_CsvIpPath)
    }

    //spark.read.format("csv").option("header", "true").option("inferSchema","true").load(dq_CsvIpPath)
  }

  def processJson(spark: SparkSession, dq_CsvIpPath:String, schemaPath: String, multi_line_flg: String ):DataFrame ={
    val readSchemaCsv = spark.sparkContext.textFile(schemaPath)
    val readSchemaCsvLines = readSchemaCsv.collect().mkString
    val newSchema = DataType.fromJson(readSchemaCsvLines).asInstanceOf[StructType]
    //Read new schema and input data
    spark.read.schema(newSchema).json(dq_CsvIpPath)
  }
}
