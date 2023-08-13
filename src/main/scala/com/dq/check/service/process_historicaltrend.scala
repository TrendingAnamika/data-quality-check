package com.dq.check.service

import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.expr

import scala.collection.immutable.Map
import org.apache.spark.sql.functions.col

import scala.util.control.NonFatal
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{LogManager, Logger}

object process_historicaltrend {
  /**
   * @return mixed
   */
  val log: Logger = Logger.getLogger(this.getClass.getName)
  var filepath_list = List.empty[String] // for handling attachment names
  val current_Timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd_HHmmss"))
  val current_Date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
  val year_month = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMM")).toInt
  var trnd_anlys_Error_Count = 0.toLong
  var thresholdcheck_counter = 0
  var generictrend_counter = 0
  var filePath_freq_report = ""
  var filePath_mean_report = ""

  def process_psitrend(dataframeLists: List[DataFrame],kvpairs:Map[String, String],argspair:Map[String,String],report_details:List[String],sheet_name:List[String],spark: SparkSession, sc: SparkContext,lowerbound:Double,upperbound:Double) = {
    //---Read user provided SQL
    //spark.sql(consolidatedSql).show()
    val appName = kvpairs("appName")

    //val db_parameters = kvpairs("db_parameters")
    //val db_parameters_1 =  db_parameters.replace("~","|")
    //val dbValuePairs = db_parameters.split("~").map(_.toInt)filepath_list
    val stability_index_table = kvpairs("stability_index_table")
    val stability_index_table_stg = kvpairs("stability_index_table_stg")

    println("processHistoricaldata")
    val appId = kvpairs("appId")
    val profiling_database = kvpairs("Output_profiling_database")
    val Dataset_Name = kvpairs("dataset_name")
    val hist_trnd_anlys_attachment_names = kvpairs("hist_trnd_anlys_attachment_names")
    val hist_trnd_anlys_attachment = hist_trnd_anlys_attachment_names.split(",").map(_.toString).distinct

    val histdf = dataframeLists(0)
    val dqMetrics = dataframeLists(1)
    val myExpression = "(value-avg_value)*log(value/avg_value)"
    val histdf_persist = histdf.persist(StorageLevel.MEMORY_ONLY_2)
    val dqMetrics_persist = dqMetrics.persist(StorageLevel.MEMORY_ONLY_2)
    var histdf_persist_dedup = histdf_persist.dropDuplicates(Seq("metric_name","column_name","time_period"))
    val w = Window.partitionBy("column_name","metric_name").orderBy("column_name")
    var histdf_avg = histdf_persist_dedup.select(col("column_name"),col("metric_name"),col("time_period"),avg("value").over(w))
    histdf_avg = histdf_avg.drop("time_period")
    val newNames = Seq("column_name", "metric_name","Avg_Value")

    val df_avg_value = histdf_avg.dropDuplicates(Seq("column_name","metric_name")).toDF(newNames: _*)
    histdf_persist_dedup = histdf_persist_dedup.withColumn("value",concat((col("value") * 100).cast("Decimal(10,6)"),lit('%'))).withColumn("time_period",concat( col("time_period"), lit("_"), lit("Percent")))
    var histdf_pivot = histdf_persist_dedup.groupBy("column_name","metric_name").pivot("time_period").agg(first("value"))

    histdf_pivot.show(5)
    //var diffDf = histdf.join(broadcast(dqMetrics_dedup), Seq("metric_name","column_name"))
    var diffDf = histdf_pivot.join(dqMetrics_persist, Seq("metric_name","column_name")).join(df_avg_value,Seq("metric_name","column_name"))
    diffDf = diffDf.withColumn("psi",when(col("Avg_Value") =!= 0, expr(myExpression)).otherwise(null))

    //diffDf = diffDf.withColumn("Status",when(col("psi") >= lowerbound && col("psi") <= upperbound ,"SUCCESS").when(col("psi") < lowerbound || col("psi") > upperbound ,"FAILURE").otherwise("NA"))
    val df_psi_final = diffDf.withColumn("Avg_Value",concat((col("Avg_Value") * 100).cast("Decimal(10,6)"),lit('%')))
      .withColumn("value",concat((col("value") * 100).cast("Decimal(10,6)"),lit('%'))).withColumn("psi", col("psi").cast("Decimal(10,7)"))
    df_psi_final.show()


    df_psi_final.createOrReplaceTempView("temphistTable_psi")
    val argspair_key = argspair.-("ip_file").-("hiveDB").-("user_config").-("env").-("exec_mode").mapValues(_.toString)
    val dbkeyColumnSeq = argspair_key.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq
    val argspair_str = argspair_key.map{case (k, v) => k + ":" + v}.mkString("|")
    DataCheckImplementation.mail_config += ("Identifier" -> argspair_key.map{case (k, v) => k + ":" + v}.mkString("\n"))
    val dqhistoryanalysis = spark.sql("select * from temphistTable_psi")
      .withColumn("Dataset_Name", lit(Dataset_Name))
      .withColumn("Status", lit("NA"))
      .withColumn("insert_timestamp", lit(current_Timestamp))
      .withColumn("appId",lit(appId))
      .withColumn("currentDate", lit(current_Date))
      .withColumn("indicator", map(dbkeyColumnSeq:_*))
      .withColumnRenamed("value", "Current_Percentage")
      .withColumnRenamed("lowerbound", "Lower_Threshold")
      .withColumnRenamed("upperbound", "Upper_Threshold")
      .withColumn("appname", lit(appName))
      .withColumn("year_month",lit(year_month))

    dqhistoryanalysis.persist(StorageLevel.MEMORY_AND_DISK)
    dqhistoryanalysis.createOrReplaceTempView("tempTable")
    val dqhistoryanalysis_selectcols = dqhistoryanalysis.select("column_name","metric_name","Current_Percentage","Avg_Value","psi","Status","Dataset_Name","insert_timestamp","appId","currentDate","indicator","appName","year_month")

    //spark.sql("create table if not exists " + profiling_database + "." + stability_index_table +  " AS select * from tempTable")

    var df_psi_summary = df_psi_final.groupBy("column_name").agg(sum("psi").alias("Stability_Index"))
    df_psi_summary = df_psi_summary.withColumn("Status",when(col("Stability_Index") >= lowerbound && col("Stability_Index") <= upperbound ,"SUCCESS").when(col("Stability_Index") < lowerbound || col("Stability_Index") > upperbound ,"FAILURE").otherwise("NA"))
    val df_psi_errors = df_psi_summary.filter(df_psi_summary("Status") === "FAILURE")
    trnd_anlys_Error_Count = trnd_anlys_Error_Count + df_psi_summary.filter(df_psi_summary("Status") === "FAILURE").count()
    ///if (df_psi_errors.count() > 0) {
    println("inside if PSI_trend")
    ///  val dqhistoryanalysis_final = dqhistoryanalysis_selectcols.withColumn("indicator", lit(argspair_str))
    ///  DataCheckImplementation.hiveSave(dqhistoryanalysis_final: DataFrame, spark: SparkSession, sc: SparkContext, profiling_database: String, stability_index_table_stg: String,"overwrite")
    ///}
    ///else {
    val partition_cols = List("appname","year_month")
    spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    DataCheckImplementation.hiveSave(dqhistoryanalysis_selectcols: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_database: String, stability_index_table: String)
    ///}

    val dqhistoryanalysis_psi = dqhistoryanalysis.withColumn("indicator", lit(argspair_str))

    //attachment handling for history trend
    val hdfs_path_for_attchmnt = hist_trnd_anlys_attachment(0)
    //var hdfs_path_for_freq_attachmnt = hist_trnd_anlys_attachment(1)
    //val filePath_psi_summary_report = Paths.get(hdfs_path_for_attchmnt, "psi_summary").toString
    //val filePath_psi_error_report = Paths.get(hdfs_path_for_attchmnt, "psi_error_report").toString

    //val reorderedColumnNames_psi: Array[String] = Array("metric_name","column_name","Current_Value","Lower_Threshold","Upper_Threshold","Status","Identifier","Dataset_Name","appName","insert_timestamp","appId","currentDate")
    val dqhistoryanalysis_psi_final: DataFrame = dqhistoryanalysis_psi.orderBy("column_name", "metric_name").drop("Calc_Status")
    dqhistoryanalysis_psi_final.show()
    println("filepath_list:" + filepath_list)

    try {
      val filePath_psi_report = SaveReport.savefile(dqhistoryanalysis_psi_final,hdfs_path_for_attchmnt,report_details,sheet_name(0),"overwrite")
      val filePath_psi_summary_report = SaveReport.savefile(df_psi_summary,hdfs_path_for_attchmnt,report_details,sheet_name(1))
      filepath_list = filepath_list :+ filePath_psi_report
      filepath_list = filepath_list :+ filePath_psi_summary_report

      if (df_psi_errors.count() > 0){
        val filePath_psi_error_report = SaveReport.savefile(df_psi_errors,hdfs_path_for_attchmnt,report_details,sheet_name(2))
        filepath_list = filepath_list :+ filePath_psi_error_report
      }
      println("filepath_list:"  + filepath_list)
    }
    catch {
      case NonFatal(e) =>
        System.err.println("DQ_Profiling_table does not have sufficient data for frequency calculation!")
    }
  }


  def process_thresholdcheck(dataframeLists: List[DataFrame],kvpairs:Map[String, String],argspair:Map[String,String],report_details:List[String],sheet_name:List[String],spark: SparkSession, sc: SparkContext): DataFrame = {
    //---Read user provided SQL
    //spark.sql(consolidatedSql).show()
    thresholdcheck_counter = thresholdcheck_counter + 1
    val appName = kvpairs("appName")

    //val db_parameters = kvpairs("db_parameters")
    //val db_parameters_1 =  db_parameters.replace("~","|")
    //val dbValuePairs = db_parameters.split("~").map(_.toInt)
    val historical_table = kvpairs("historical_table")
    val historical_table_stg = kvpairs("historical_table_stg")


    println("processHistoricaldata")
    val appId = kvpairs("appId")
    val profiling_database = kvpairs("Output_profiling_database")
    val Dataset_Name = kvpairs("dataset_name")

    val hist_trnd_anlys_attachment_names = kvpairs("hist_trnd_anlys_attachment_names")
    val hist_trnd_anlys_attachment = hist_trnd_anlys_attachment_names.split(",").map(_.toString).distinct

    val histdf = dataframeLists(0)
    val dqMetrics = dataframeLists(1)
    val histdf_persist = histdf.persist(StorageLevel.MEMORY_ONLY_2)
    val dqMetrics_persist = dqMetrics.persist(StorageLevel.MEMORY_ONLY_2)
    val dqMetrics_dedup = dqMetrics_persist.dropDuplicates(Seq("metric_name","column_name"))
    //var diffDf = histdf.join(broadcast(dqMetrics_dedup), Seq("metric_name","column_name"))
    var diffDf = histdf_persist.join(dqMetrics_dedup, Seq("metric_name","column_name"))
    diffDf = diffDf.withColumn("Calc_Status",when(col("value") >= col("lowerbound") && col("value") <= col("upperbound") ,"SUCCESS").when(col("value") < col("lowerbound") || col("value") > col("upperbound") ,"FAILURE").otherwise("NA"))
    diffDf = diffDf.withColumn("Final_Status",lit("NA")).withColumn("support_override",lit("NA"))

    diffDf.createOrReplaceTempView("temphistTable_threshold")
    val argspair_key = argspair.-("ip_file").-("hiveDB").-("user_config").-("env").-("exec_mode").-("process_date").mapValues(_.toString)
    val dbkeyColumnSeq = argspair_key.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq
    val argspair_str = argspair_key.map{case (k, v) => k + ":" + v}.mkString("|")
    DataCheckImplementation.mail_config += ("Identifier" -> argspair_key.map{case (k, v) => k + ":" + v}.mkString("\n"))
    val dqhistoryanalysis = spark.sql("select * from temphistTable_threshold")
      .withColumn("Dataset_Name", lit(Dataset_Name))
      .withColumn("insert_timestamp", lit(current_Timestamp))
      .withColumn("appId",lit(appId))
      .withColumn("currentDate", lit(current_Date))
      .withColumn("indicator", map(dbkeyColumnSeq:_*))
      .withColumnRenamed("value", "Current_Percentage")
      .withColumnRenamed("lowerbound", "Lower_Threshold")
      .withColumnRenamed("upperbound", "Upper_Threshold")
      .withColumn("appname", lit(appName))
      .withColumn("year_month",lit(year_month))

    dqhistoryanalysis.persist(StorageLevel.MEMORY_AND_DISK)
    dqhistoryanalysis.createOrReplaceTempView("tmpTbl_dqhistoryanalysis")
    //spark.sql("create table if not exists " + profiling_database + "." + historical_table +  " AS select * from tempTable1")
    ///dqhistoryanalysis.show(1, false)
    trnd_anlys_Error_Count = trnd_anlys_Error_Count + dqhistoryanalysis.filter(dqhistoryanalysis("Calc_Status") === "FAILURE").count()

    //data.write.mode("append").parquet(hdfs_path_dq_status)
    //spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    //dqhistoryanalysis.write.mode("append").insertInto(profiling_database + "." + historical_table)
    //log.info("The historical table has successfully been wrote into  -----> " + profiling_database + "." + historical_table)
    val df_errors = dqhistoryanalysis.filter(dqhistoryanalysis("Calc_Status") === "FAILURE")

    if (df_errors.count() > 0) {
      val partition_cols = List("indicator","year_month")
      val dqhistoryanalysis_final = dqhistoryanalysis.withColumn("indicator", lit(argspair_str))
      spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

      DataCheckImplementation.hiveSave_stg(dqhistoryanalysis_final: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_database: String, historical_table_stg: String,"overwrite")
    }
    else {
      val partition_cols = List("appname","year_month")
      val dqhistoryanalysis_final = dqhistoryanalysis.withColumn("indicator", map(dbkeyColumnSeq:_*))
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

      DataCheckImplementation.hiveSave(dqhistoryanalysis_final: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_database: String, historical_table: String)
    }
    val dqhistoryanalysis_mean = dqhistoryanalysis.withColumn("indicator", lit(argspair_str)).withColumnRenamed("Current_Percentage", "Current_Value")

    //attachment handling for history trend
    var hdfs_path_for_attchmnt = hist_trnd_anlys_attachment(0)
    //var hdfs_path_for_freq_attachmnt = hist_trnd_anlys_attachment(1)
    //val filePath_mean_report = Paths.get(hdfs_path_for_attchmnt, "threshold_chk.csv").toString
    filePath_mean_report = Paths.get(hdfs_path_for_attchmnt, "threshold_chk").toString

    //hdfs_path_for_attchmnt = hdfs_path_for_attchmnt.splitAt(hdfs_path_for_attchmnt.lastIndexOf("/"))._1

    val reorderedColumnNames_mean: Array[String] = Array("metric_name","column_name","Current_Value","Lower_Threshold","Upper_Threshold","Calc_Status","Final_Status","support_override","indicator","Dataset_Name","appName","insert_timestamp","appId","currentDate")
    val dqhistoryanalysis_mean_final: DataFrame = dqhistoryanalysis_mean.select(reorderedColumnNames_mean.head, reorderedColumnNames_mean.tail: _*)

    filePath_mean_report = filePath_mean_report.concat("/threshold_chk.csv")
    filepath_list = filepath_list :+ filePath_mean_report

    return dqhistoryanalysis_mean_final
  }
  def process_generictrend(dataframeLists: List[DataFrame],kvpairs:Map[String, String],argspair:Map[String,String],report_details:List[String],sheet_name:List[String],spark: SparkSession, sc: SparkContext):DataFrame = {
    //---Read user provided SQL
    //spark.sql(consolidatedSql).show()
    val appName = kvpairs("appName")
    generictrend_counter = generictrend_counter + 1

    //val db_parameters = kvpairs("db_parameters")
    //val db_parameters_1 =  db_parameters.replace("~","|")
    //val dbValuePairs = db_parameters.split("~").map(_.toInt)
    val historical_table = kvpairs("historical_table")
    val historical_table_stg = kvpairs("historical_table_stg")

    println("processHistoricaldata")
    val appId = kvpairs("appId")
    val profiling_database = kvpairs("Output_profiling_database")
    val Dataset_Name = kvpairs("dataset_name")
    val hist_trnd_anlys_attachment_names = kvpairs("hist_trnd_anlys_attachment_names")
    val hist_trnd_anlys_attachment = hist_trnd_anlys_attachment_names.split(",").map(_.toString).distinct

    val histdf = dataframeLists(0)
    val dqMetrics = dataframeLists(1)
    val histdf_persist = histdf.persist(StorageLevel.MEMORY_ONLY_2)
    //log.info("--- Inside Object: process_historicaltrend, Routine: process_generictrend -- show histdf_persist dataframe --- "+ histdf_persist.show(10,false))
    val dqMetrics_persist = dqMetrics.persist(StorageLevel.MEMORY_ONLY_2)

    val dqMetrics_dedup = dqMetrics_persist.dropDuplicates(Seq("metric_name","column_name"))
    var diffDf = histdf_persist.join(dqMetrics_dedup, Seq("metric_name","column_name"))
    //log.info("--- Inside Object: process_historicaltrend, Routine: process_generictrend -- show after join hist vs curr df --- "+ diffDf.show(10,false))
    diffDf = diffDf.withColumn("Calc_Status",when(col("value") >= col("lowerbound") && col("value") <= col("upperbound") ,"SUCCESS").when(col("value") < col("lowerbound") || col("value") > col("upperbound") ,"FAILURE").otherwise("NA"))
    diffDf = diffDf.withColumn("Final_Status",lit("NA")).withColumn("support_override",lit("NA"))

    diffDf.createOrReplaceTempView("temphistTable_generic")
    val argspair_key = argspair.-("ip_file").-("hiveDB").-("user_config").-("env").-("exec_mode").mapValues(_.toString)
    val dbkeyColumnSeq = argspair_key.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq
    val argspair_str = argspair_key.map{case (k, v) => k + ":" + v}.mkString("|")
    DataCheckImplementation.mail_config += ("Identifier" -> argspair_key.map{case (k, v) => k + ":" + v}.mkString("\n"))
    val dqhistoryanalysis = spark.sql("select * from temphistTable_generic")
      .withColumn("Dataset_Name", lit(Dataset_Name))
      .withColumn("insert_timestamp", lit(current_Timestamp))
      .withColumn("appId",lit(appId))
      .withColumn("currentDate", lit(current_Date))
      .withColumn("indicator", map(dbkeyColumnSeq:_*))
      .withColumnRenamed("value", "Current_Percentage")
      .withColumnRenamed("lowerbound", "Lower_Threshold")
      .withColumnRenamed("upperbound", "Upper_Threshold")
      .withColumn("appname", lit(appName))
      .withColumn("year_month",lit(year_month))

    dqhistoryanalysis.persist(StorageLevel.MEMORY_AND_DISK)

    //log.info("--- Inside Object: process_historicaltrend, Routine: process_generictrend -- df after analysis --- "+ dqhistoryanalysis.show(10, false))

    //dqhistoryanalysis.createOrReplaceTempView("tmpTbl_dqhistoryanalysis")
    //spark.sql("create table if not exists " + profiling_database + "." + historical_table +  " AS select * from tempTable1")
    trnd_anlys_Error_Count = trnd_anlys_Error_Count + dqhistoryanalysis.filter(dqhistoryanalysis("Calc_Status") === "FAILURE").count()   //arvinth
    val df_errors = dqhistoryanalysis.filter(dqhistoryanalysis("Calc_Status") === "FAILURE")

    if (df_errors.count() > 0) {
      val partition_cols = List("indicator","year_month")
      val dqhistoryanalysis_final = dqhistoryanalysis.withColumn("indicator",lit(argspair_str))
      spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

      DataCheckImplementation.hiveSave_stg(dqhistoryanalysis_final: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_database: String, historical_table_stg: String, "overwrite")
    }
    else {
      val partition_cols = List("appname","year_month")
      val dqhistoryanalysis_final = dqhistoryanalysis.withColumn("indicator", map(dbkeyColumnSeq:_*))
      spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

      DataCheckImplementation.hiveSave(dqhistoryanalysis_final: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_database: String, historical_table: String)
    }
    val split_col = split(dqhistoryanalysis("metric_name"), "_")
    val df_final_freq = dqhistoryanalysis.withColumn("metric_name1", split_col.getItem(0)).withColumn("Value", split_col.getItem(1))
      .drop("metric_name").withColumnRenamed("metric_name1", "Metric_name").withColumn("indicator", lit(argspair_str))
    var hdfs_path_for_attchmnt = hist_trnd_anlys_attachment(0)
    filePath_freq_report = Paths.get(hdfs_path_for_attchmnt, "freq").toString
    log.info("--- Inside Object: process_historicaltrend, Routine: process_generictrend -- print var filePath_freq_report --- "+ filePath_freq_report)
    val columns: Array[String] = df_final_freq.columns
    val reorderedColumnNames: Array[String] = Array("column_name","Metric_name","Value","Current_Percentage","Lower_Threshold","Upper_Threshold","Calc_Status","Final_status","support_override","indicator","Dataset_Name","appName","insert_timestamp","appId","currentDate")
    val df_final_freq_final: DataFrame = df_final_freq.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    filePath_freq_report = filePath_freq_report.concat("/freq.csv")
    filepath_list = filepath_list :+ filePath_freq_report
    log.info("--- Inside Object: process_historicaltrend, Routine: process_generictrend -- print var filepath_list --- "+ filepath_list)
    return df_final_freq_final
  }
}
