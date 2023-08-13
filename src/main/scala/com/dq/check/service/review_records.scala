package com.dq.check.service


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.expr

import scala.collection.immutable.Map

import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{LogManager, Logger}

object review_records {
  /**
   * @return mixed
   */
  val log: Logger = Logger.getLogger(this.getClass.getName)
  var filepath_list = List.empty[String] // for handling attachment names
  val current_Timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd_HHmmss"))
  val current_Date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
  val year_month = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMM"))
  var trnd_anlys_Error_Count = 0.toLong
  var thresholdcheck_counter = 0
  var generictrend_counter = 0
  var filePath_freq_report = ""
  var filePath_mean_report = ""


  def process_thresholdcheck(args: Array[String], spark: SparkSession, sc: SparkContext, log: Logger, argspair: Map[String,String], exec_mode: String) = {

    //spark.sql(consolidatedSql).show()
    val kvpairs = args.grouped(2).collect { case Array(k, v) => k -> v }.toMap
    log.info("kvparis for file processing-----" + kvpairs)
    println(kvpairs)
    val review_status = argspair("review_status").toLowerCase
    val argspair_key = argspair.-("ip_file").-("hiveDB").-("user_config").-("env").-("exec_mode").-("process_date").-("review_status").mapValues(_.toString)
    val dbkeyColumnSeq = argspair_key.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq
    val argspair_str = argspair_key.map{case (k, v) => k + ":" + v}.mkString("|")
    //val dbkey: Map[Int, Int] = dbValuePairs.map(x => x).zipWithIndex.map(t => (t._2, t._1)).toMap
    //val dbkeyColumnSeq = dbkey.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq
    var df_final = Seq[DataFrame]()
    val profiling_db = kvpairs("Output_profiling_database")
    val historical_table = kvpairs("historical_table")
    val historical_table_stg = kvpairs("historical_table_stg")
    val Stability_index_table = kvpairs("stability_index_table")
    val Stability_index_table_stg = kvpairs("stability_index_table_stg")
    val source_table = if (exec_mode == "review"){
      historical_table_stg
    } else if (exec_mode == "review_psi"){
      Stability_index_table_stg
    }
    val final_table = if (exec_mode == "review"){
      historical_table
    } else if (exec_mode == "review_psi"){
      Stability_index_table
    }

    var threshold_chk_seq = Seq[DataFrame]()
    var curr_sql = "select * from <db.table> where indicator = '" + argspair_str + "'"
    curr_sql = curr_sql.replaceAll("<db.table>", profiling_db + "." + source_table)
    println("curr_sql: " + curr_sql)
    val df_curr = spark.sql(curr_sql)
    DataCheckImplementation.mail_config += ("Identifier" -> argspair_key.map { case (k, v) => k + ":" + v }.mkString("\n"))
    df_curr.createOrReplaceTempView("temphistTable_review")

    val dqhistoryanalysis = spark.sql("select * from temphistTable_review")
      .withColumn("insert_timestamp", lit(current_Timestamp))
      .withColumn("currentDate", lit(current_Date))
      .withColumn("indicator", map(dbkeyColumnSeq:_*))
    val partition_cols = List("appname","year_month")
    if (review_status == "pass") {
      val dqhistoryanalysis_review = dqhistoryanalysis.withColumn("Final_Status", lit("SUCCESS"))
        .withColumn("support_override", lit("TRUE"))
      //spark.sql("create table if not exists " + profiling_database + "." + historical_table +  " AS select * from tempTable1")
      dqhistoryanalysis_review.show(10, false)
      DataCheckImplementation.hiveSave(dqhistoryanalysis_review: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_db: String, final_table.toString: String)
    }
    else if (review_status == "fail") {
      val dqhistoryanalysis_review = dqhistoryanalysis.withColumn("Final_Status", lit("FAILURE"))
        .withColumn("support_override", lit("TRUE"))
      dqhistoryanalysis_review.show(10, false)
      DataCheckImplementation.hiveSave(dqhistoryanalysis_review: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, profiling_db: String, final_table.toString: String)
    }
    else {
      //spark.sql("create table if not exists " + profiling_database + "." + historical_table +  " AS select * from tempTable1")
      val dqhistoryanalysis_review = dqhistoryanalysis.withColumn("Final_Status", lit("FAILURE"))
        .withColumn("support_override", lit("TRUE"))
    }

    //data.write.mode("append").parquet(hdfs_path_dq_status)
    //spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    //dqhistoryanalysis.write.mode("append").insertInto(profiling_database + "." + historical_table)
    //log.info("The historical table has successfully been wrote into  -----> " + profiling_database + "." + historical_table)
  }
}
