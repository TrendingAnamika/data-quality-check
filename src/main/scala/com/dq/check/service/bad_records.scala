package com.dq.check.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.Map
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{LogManager, Logger}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.io.Source

object bad_records {
  /**
   * @return mixed
   */
  val log: Logger = Logger.getLogger(this.getClass.getName)
  var filepath_list = List.empty[String] // for handling attachment names
  val current_Timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd_HHmmss"))
  val current_Date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
  val year_month = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMM"))


  def process_recordcheck(args: Array[String], rowCheckers: List[RowChecker], spark: SparkSession, sc: SparkContext, log: Logger, argspair: Map[String,String]):String = {
    // Check data quality measures -- this is only for generic DQ report
    println("Inside Bad_Records_report")
    val datetimeNow = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))
    val kvpairs = args.grouped(2).collect { case Array(k, v) => k -> v }.toMap
    log.info("kvparis for file processing-----" + kvpairs)
    println(kvpairs)
    val ip_file = kvpairs("input_file")
    val formatType = kvpairs("format")
    val dq_IpHiveQuery = kvpairs("dq_IpHiveQuery")
    var table_string = kvpairs("table_string")
    ///val opFilePath = kvpairs("opFilePath")
    val appName = kvpairs("appName")
    val ipSchemaPath = kvpairs("ipSchemaPath")
    val Outputhive_table = kvpairs("Outputhive_table")
    val dqChkConsOpPath = kvpairs("dqChkConsOpPath")
    val hiveDB = kvpairs("hiveDB")
    println("qualityResult")
    val val_sep = kvpairs("val_sep")
    val val_header = kvpairs("val_header")
    var val_infr_schema = kvpairs("val_infr_schema")
    val appId = kvpairs("appId")
    val val_null_vl = kvpairs("val_null_vl")
    val val_charset = kvpairs("val_charset")
    val outputBaseFilename = ip_file.split("/").last.split("\\.")(0)
    val outputFilename = "%s/%s/%s_stats".format(dqChkConsOpPath, datetimeNow, outputBaseFilename)
    var outputFilenameFailingRows = "%s/%s/%s_failing_rows".format(dqChkConsOpPath, datetimeNow, outputBaseFilename)
    val preprocess_Path = kvpairs("preprocess_Path")
    val profiling_records = kvpairs("profiling_records")
    val dataset_name = kvpairs("dataset_name")
    val multi_line_flg = kvpairs("multi_line_flg")
    val Dataset_Multi_Line_quote = kvpairs("Dataset_Multi_Line_quote")
    val Dataset_Multi_Line_escape = kvpairs("Dataset_Multi_Line_escape")

    var consolidatedSql=dq_IpHiveQuery
    val input_data = if (formatType == "hive")
    {
      //dq_IpHiveQuery=broadcastVar_proc_dt
      val filename=dq_IpHiveQuery.split("/").last
      consolidatedSql = Source.fromFile(filename).getLines.mkString
      //.replace("<process_date>",process_date)
      for ((k,v) <- argspair) consolidatedSql = consolidatedSql.replaceAll("<"+k+">",v)
      consolidatedSql
    } else {ip_file }
    log.info("log: input_data------" + input_data)

    val df = DataQualityFactoryPattern.determineFileFormat(formatType, spark, sc, ip_file, ipSchemaPath, consolidatedSql, val_sep, val_header, val_infr_schema, val_null_vl, val_charset, multi_line_flg, Dataset_Multi_Line_quote, Dataset_Multi_Line_escape)

    val df_col_list = df.columns.toArray //will be used in UDF

    val count_df = df.count()
    log.info("Rowcount of Spark dataframe-----" + count_df)

    if(count_df == 0){
      //If input data contains zero records, then stop the execution and notify user.
      log.info("There is no input data available to perform DQ. Exiting the script. Please check the source table or input file for data availability")
      System.out.println("There is no input data available to perform DQ. Exiting the script. Please check the source table or input file for data availability")
      //val err_msg = "There is no input data available to perform DQ. Exiting the script. Please check the source table or input file for data availability"
      //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
      //System.exit(0) //Exit the DQ job gracefully
      return("noRecordsFound")
    }

    df.show(10, false)
    val current_Timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd_HHmmss"))
    val current_Date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))


    //val dqCheckStagingTblOp = spark.sql("select * from dqCheckStagingTbl")
    var dqCheckStagingTblOp = spark.sql("select Constraint_rule,Validation_rule,constraint_msg,resulting_status,Total_rows,Pass_count,Pass_percentage,Fail_count,Fail_percentage,uniq_rule_identifier,rule_id from dqCheckStagingTbl")
      .withColumn("input_data", lit(input_data))
      .withColumn("Hive_inputTable", lit(table_string))
      .withColumn("insert_timestamp", lit(from_unixtime(unix_timestamp(lit(current_Timestamp), "yyyy-MM-dd_hhmmss"))))
      .withColumn("job_run_id",lit(appId))
      .withColumn("Dataset_name",lit(dataset_name))
      //.withColumn("rule_name",extrctRuleName(col("validation_rule")))
      .withColumn("currentDate", lit(from_unixtime(unix_timestamp(lit(current_Date), "yyyy-MM-dd"))))
    dqCheckStagingTblOp.createOrReplaceTempView("tempTable")
    //.withColumn("appName", lit(appName))
    //withColumn("Trend_key_hash",lit((concat(lit(appName),lit("_"),col("validation_rule"))).hashCode().toString()))
    print("---------------------------Target Schema-----------------------")
    dqCheckStagingTblOp.printSchema()
    dqCheckStagingTblOp.show(10,false)
    print(dqCheckStagingTblOp.printSchema())
    DataCheckImplementation.hiveInsert(dqCheckStagingTblOp: DataFrame, spark: SparkSession, sc: SparkContext, hiveDB: String, Outputhive_table: String)


    // Find failing row
    if (kvpairs("save_failing_rows").toBoolean) {
      val fr = new FindFaillingRowsGlobal

      val failingRows = fr.find(df, spark, sc, rowCheckers)

      failingRows
        .withColumn("QualityCheckDate", lit(datetimeNow))
        .repartition(1)
        .write.format("parquet")
        .mode("append")
        .save(outputFilenameFailingRows)
    }
    return("qc_port_completed_successfully")
  }
}