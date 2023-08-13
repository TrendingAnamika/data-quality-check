package com.dq.check.service

import com.dq.check.service.process_historicaltrend.process_thresholdcheck
import com.dq.check.service.{Report_thread, bad_records, process_historicaltrend}

import java.io.File
import java.io.IOException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigException.Missing
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{LogManager, Logger, PatternLayout, RollingFileAppender}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.io.Source
import scala.util.control.NonFatal
import org.apache.spark.launcher.SparkLauncher

import scala.collection.JavaConverters._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{array, col, explode, from_unixtime, get_json_object, isnull, json_tuple, lit, unix_timestamp}

import scala.collection.immutable.Map
import scala.util.parsing.json.JSON
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

object DataCheckImplementation {

  //val sndEmail = new SendEmail
  val log: Logger = Logger.getLogger(this.getClass.getName)
  var errMailGlblConfig = scala.collection.mutable.Map[String, String]()

  //val log: Logger = LogManager.getLogger(DataCheckImplementation.getClass)

  var dq_error_sent_to = ""
  //PropertyConfigurator.configure("log4j.properties")
  var rule_status = "" // rule or constraint execution status
  var mail_config = scala.collection.mutable.Map[String, String]()

  val start = 300000
  val end = 500000
  val rnd = new scala.util.Random


  def hiveInsert(data_df: DataFrame, spark: SparkSession, sc: SparkContext, hiveDB: String, Outputhive_table: String): Unit = {
    //This function will re-try inserting into hive table for a max of three times
    val insrt_countr = sc.longAccumulator("Hive insert attempt counter")
    var insertFlag: Boolean = false
    //insrt_countr.add(1)

    while (insrt_countr.value <= 3 && !insertFlag) {
      Try {
        insrt_countr.add(1)
        log.info("Inside hiveInsert routine for inserting values into  -----> " + hiveDB + "." + Outputhive_table + "insrt_countr value: " + insrt_countr)
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        data_df
          .coalesce(1)
          .write
          .mode("append").insertInto(hiveDB + "." + Outputhive_table)
      }
      match {
        case Success(v) =>
          log.info("The data successfully inserted into  -----> " + hiveDB + "." + Outputhive_table)
          log.info("The schema for "+ hiveDB + "." + Outputhive_table + " " + data_df.printSchema())
          insertFlag = true
        //break the loop.
        case Failure(e: Throwable) =>
          log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Insert attempt number " + insrt_countr.value + " of 3. Error message: " + e.printStackTrace())
          log.info("The schema for "+ hiveDB + "." + Outputhive_table + " " + data_df.printSchema())
          Thread.sleep(start + rnd.nextInt( (end - start) + 1 ))
      }
    }

    log.info("outside hiveInsert while loop")

    if (!insertFlag) {
      log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts ")
      val err_msg = "Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts. Please check the spark log for details."
      //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
      System.exit(1)
    }
  }

  def hiveSave_profiling(data_df: DataFrame, partition_cols:List[String], spark: SparkSession, sc: SparkContext, hiveDB: String, Outputhive_table: String,savemode:String = "append"): Unit = {
    //This function will re-try inserting into hive table for a max of three times
    val insrt_countr = sc.longAccumulator("Hive insert attempt counter")
    var insertFlag: Boolean = false
    //insrt_countr.add(1)
    while (insrt_countr.value <= 2 && !insertFlag && data_df.count() > 0) {
      Try {
        insrt_countr.add(1)
        log.info("Inside hiveSave routine for inserting values into  -----> " + hiveDB + "." + Outputhive_table + "insrt_countr value: " + insrt_countr)
        //spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.sql(s"use $hiveDB")
        //spark.conf.set("hive.exec.dynamic.partition", "true")
        data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
        //spark.sql("select appname, year_month from final_data limit 1").show()
        val app_name = spark.sql("select appname from final_data limit 1").collect().map(_(0)).toList

        println("partn_col_values: "+ app_name(0))

        val year_month = spark.sql("select year_month from final_data limit 1").collect().map(_(0)).toList
        println("year_month: "+ year_month(0))

        //data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
        spark.sql("INSERT INTO "+ hiveDB +"."+ Outputhive_table +" PARTITION(appname='"+ app_name(0) +"', year_month="+ year_month(0) +") SELECT * FROM"+
          "(SELECT  entity,column_name,metric_name,Metric_Abs_Value,Metric_Ratio_Value,Dataset_Name,insert_timestamp,appId,currentDate,indicator from final_data) final_data")

        data_df.printSchema()
        println(data_df.printSchema())
        data_df.show()

        //data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*)
        //data_df.repartition(1).write.format("hive").insertInto(Outputhive_table)
        //data_df.repartition(1).write.mode("append").format("hive").insertInto(Outputhive_table) //.mode(savemode)
      }
      match {
        case Success(v) =>
          log.info("The data successfully inserted into  -----> " + hiveDB + "." + Outputhive_table)
          insertFlag = true

        case Failure(e: Throwable) =>
          log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Insert attempt number " + insrt_countr.value + " of 3. Error message: " + e.printStackTrace())
          Thread.sleep(start + rnd.nextInt( (end - start) + 1 ))
      }
    }

    log.info("outside hiveInsert while loop")

    if (!insertFlag && data_df.count() > 0) {
      log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts ")
      val err_msg = "Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts. Please check the spark log for details."
      //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
      System.exit(1)
    }
  }


  def hiveSave_stg(data_df: DataFrame, partition_cols:List[String], spark: SparkSession, sc: SparkContext, hiveDB: String, Outputhive_table: String,savemode:String = "append"): Unit = {
    //This function will re-try inserting into hive table for a max of three times
    val insrt_countr = sc.longAccumulator("Hive insert attempt counter")
    var insertFlag: Boolean = false
    //insrt_countr.add(1)
    while (insrt_countr.value <= 2 && !insertFlag && data_df.count() > 0) {
      Try {
        insrt_countr.add(1)
        log.info("Inside hiveSave routine for inserting values into  -----> " + hiveDB + "." + Outputhive_table + "insrt_countr value: " + insrt_countr)
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.sql(s"use $hiveDB")

        //spark.conf.set("hive.exec.dynamic.partition", "true")
        data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
        //spark.sql("select indicator, year_month from final_data limit 1").show()
        val indicator = spark.sql("select indicator from final_data limit 1").collect().map(_(0)).toList
        println("partn_col_values: "+ indicator(0))

        val year_month = spark.sql("select year_month from final_data limit 1").collect().map(_(0)).toList
        println("year_month: "+ year_month(0))
        //data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
        spark.sql("INSERT INTO "+ hiveDB +"."+ Outputhive_table +" PARTITION(indicator='"+ indicator(0) +"', year_month="+ year_month(0) +") SELECT * FROM"+
          "(SELECT  column_name, metric_name, current_percentage, lower_threshold, upper_threshold, calc_status, "+
          "final_status, support_override, dataset_name, insert_timestamp, appid, currentdate, appname "+ "from final_data) final_data")

        data_df.printSchema()
        println(data_df.printSchema())
        data_df.show()

        //data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*)
        //data_df.repartition(1).write.format("hive").insertInto(Outputhive_table)
        //data_df.repartition(1).write.mode("append").format("hive").insertInto(Outputhive_table) //.mode(savemode)
      }

      match {
        case Success(v) =>
          log.info("The data successfully inserted into  -----> " + hiveDB + "." + Outputhive_table)
          insertFlag = true
        case Failure(e: Throwable) =>
          log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Insert attempt number " + insrt_countr.value + " of 3. Error message: " + e.printStackTrace())
          Thread.sleep(start + rnd.nextInt( (end - start) + 1 ))
      }
    }

    log.info("outside hiveInsert while loop")
    if (!insertFlag && data_df.count() > 0) {
      log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts ")
      val err_msg = "Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts. Please check the spark log for details."
      //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
      System.exit(1)
    }
  }

  def hiveSave(data_df: DataFrame, partition_cols:List[String], spark: SparkSession, sc: SparkContext, hiveDB: String, Outputhive_table: String,savemode:String = "append"): Unit = {
    //This function will re-try inserting into hive table for a max of three times
    val insrt_countr = sc.longAccumulator("Hive insert attempt counter")
    var insertFlag: Boolean = false
    //insrt_countr.add(1)
    while (insrt_countr.value <= 2 && !insertFlag && data_df.count() > 0) {
      Try {
        insrt_countr.add(1)
        log.info("Inside hiveSave routine for inserting values into  -----> " + hiveDB + "." + Outputhive_table + "insrt_countr value: " + insrt_countr)
        //spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.sql(s"use $hiveDB")

        //spark.conf.set("hive.exec.dynamic.partition", "true")
        data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
        //spark.sql("select appname, year_month from final_data limit 1").show()

        val app_name = spark.sql("select appname from final_data limit 1").collect().map(_(0)).toList
        println("partn_col_values: "+ app_name(0))

        val year_month = spark.sql("select year_month from final_data limit 1").collect().map(_(0)).toList
        println("year_month: "+ year_month(0))

        //data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*).createOrReplaceTempView("final_data")
        spark.sql("INSERT INTO "+ hiveDB +"."+ Outputhive_table +" PARTITION(appname='"+ app_name(0) +"', year_month="+ year_month(0) +") SELECT * FROM"+
          "(SELECT  column_name, metric_name, current_percentage, lower_threshold, upper_threshold, calc_status, "+
          "final_status, support_override, dataset_name, insert_timestamp, appid, currentdate, indicator "+ "from final_data) final_data")

        data_df.printSchema()
        println(data_df.printSchema())
        data_df.show()

        //data_df.select(spark.table(s"$Outputhive_table").columns.map(col): _*)
        //data_df.repartition(1).write.format("hive").insertInto(Outputhive_table)
        //data_df.repartition(1).write.mode("append").format("hive").insertInto(Outputhive_table) //.mode(savemode)
      }
      match {
        case Success(v) =>
          log.info("The data successfully inserted into  -----> " + hiveDB + "." + Outputhive_table)
          insertFlag = true
        case Failure(e: Throwable) =>
          log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Insert attempt number " + insrt_countr.value + " of 3. Error message: " + e.printStackTrace())
          Thread.sleep(start + rnd.nextInt( (end - start) + 1 ))
      }
    }

    log.info("outside hiveInsert while loop")

    if (!insertFlag && data_df.count() > 0) {

      log.error("Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts ")

      val err_msg = "Error occurred while inserting the data  -----> " + hiveDB + "." + Outputhive_table + " Exceeded maximum number of retry attempts. Please check the spark log for details."
      //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
      System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    log.info("Reading Property file")
    try {
      //var frmwrk_config = ConfigFactory.parseFile(new File("framework_2.01.conf"))
      var spark_args = args.map(_.split("=") match { case Array(x, y) => (x, y) }).toMap
      var run_date = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)
      var user_config = ConfigFactory.parseFile(new File(spark_args("user_config").split("/").last))
      val process_date = spark_args.getOrElse("process_date", run_date)
      val ip_file = spark_args.getOrElse("ip_file", "")
      //val hiveDeequStatDb = spark_args("hiveDeequStatDb")
      //val hiveDB = spark_args("hiveDeequStatDb")
      val env = spark_args("env") //Execution environment. Ex: Dev/uat/prod
      val exec_mode = spark_args("exec_mode")
      val current_Date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
      val startTimeStamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss").format(LocalDateTime.now)
      val jobStartTime = startTimeStamp.toString
      // Parse arguments
      println("-----------------Framework config------------------")
      var frmwrk_config = ConfigFactory.parseFile(new File(user_config.getString("Input_data.frwrk_config_file").split("/").last))
      println(frmwrk_config)
      val hiveDeequStatTbl = frmwrk_config.getString("Output_data.DQ_JOB_status_Table")
      val Outputhive_table = frmwrk_config.getString("Output_data.Output_Table")
      val dq_rule_mail_sub = frmwrk_config.getString("DQ_rule_related_mail_config.mail_sub")
      val dq_rule_mail_body = frmwrk_config.getString("DQ_rule_related_mail_config.mail_body")
      val data_profile_mail_sub = frmwrk_config.getString("Data_profile_mail_config.mail_sub")
      val data_profile_mail_body = frmwrk_config.getString("Data_profile_mail_config.mail_body")
      val hist_trnd_anlys_mail_sub = frmwrk_config.getString("Hist_trnd_anlys_mail_config.mail_sub")
      val hist_trnd_anlys_mail_body = frmwrk_config.getString("Hist_trnd_anlys_mail_config.mail_body")
      val hiveDeequStatDb, hiveDB = frmwrk_config.getString("Output_data.Output_Database_".concat(env))
      var spark_log_path = frmwrk_config.getString("Output_data.spark_log_path_".concat(env))
      var err_mail_sub = frmwrk_config.getString("err_mail_config.err_mail_sub")
      var err_mail_body = frmwrk_config.getString("err_mail_config.err_mail_body")
      val indicator = spark_args.-("ip_file").-("hiveDB").-("user_config").-("env").-("exec_mode").-("process_date").-("review_status").mapValues(_.toString)
      val indicator_str = indicator.map{case (k, v) => k + "=" + v}.mkString(" ")

      //var hdfs_path_dq_status = frmwrk_config.getString("Output_data.hdfs_path_dq_status_".concat(env))
      //var attachment_names = frmwrk_config.getString("Output_data.attachment_names").replace("<fid>",System.getProperty("user.name"))
      println("-----------------user config------------------")
      println(user_config)
      dq_error_sent_to = user_config.getString("err_mail_config.err_send_to_".concat(env))
      errMailGlblConfig += ("err_mail_sub" -> err_mail_sub,
        "err_mail_body" -> err_mail_body,
        "dq_error_sent_to" -> dq_error_sent_to,
        "env" -> env,
        "run_date" -> DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now))
      var jobApplicationName = user_config.getString("spark.jobApplicationName")
      val formatType = user_config.getString("Input_data.Dataset_Type")
      val dq_IpHiveQuery = user_config.getString("Input_data.Dataset_Input_Query_".concat(env))
      val ipSchemaPath = user_config.getString("Input_data.Dataset_Custom_Schema")
      val val_sep = user_config.getString("Input_data.Dataset_Column_Separator")
      val val_header = user_config.getString("Input_data.Dataset_Header_Row_Flag")
      val val_infr_schema = user_config.getString("Input_data.Dataset_Infer_Schema")
      val val_null_vl = user_config.getString("Input_data.Dataset_NULL_sub_Value")
      val val_charset = user_config.getString("Input_data.Dataset_Charset")
      val multi_line_flg = try{user_config.getString("Input_data.Dataset_Multi_Line_Flg")} catch{case _: Missing => "false"}
      val Dataset_Multi_Line_quote = try{user_config.getString("Input_data.Dataset_Multi_Line_quote")} catch{case _: Missing => ""}
      val Dataset_Multi_Line_escape = try{user_config.getString("Input_data.Dataset_Multi_Line_escape")} catch{case _: Missing => ""}
      val dataset_name = user_config.getString("Input_data.Dataset_Name_".concat(env))
      val dq_results_sent_to = user_config.getString("dq_results_mail_config.dq_results_send_to_".concat(env))
      var data_profile_attachment_names = user_config.getString("Output_data.data_profile_attachment_names").replace("<fid>", System.getProperty("user.name"))
      var hist_trnd_anlys_attachment_names = user_config.getString("Output_data.hist_trnd_anlys_attachment_names").replace("<fid>", System.getProperty("user.name"))
      var hdfs_path_for_dq_result = user_config.getString("Output_data.hdfs_path_for_dq_result").replace("<fid>", System.getProperty("user.name"))

      var bad_records_attachment_names = try {
        user_config.getString("Output_data.bad_records_attachment_names")
      } catch {
        case _: Missing => "" // If the flag is missing, set the default value as true i.e. it will trigger email
      }

      val trigger_success_mail_flg_dq = try {
        user_config.getString("dq_results_mail_config.trigger_success_mail_flg_dq")
      } catch {
        case _: Missing => "true" // If the flag is missing, set the default value as true i.e. it will trigger email
      }

      val kill_job_for_zero_input_records_flg_dq = try {
        user_config.getString("dq_results_mail_config.kill_job_for_zero_input_records_flg_dq")
      } catch {
        case _: Missing => "true" // If the flag is missing, set the default value as true i.e. it will kill the job
      }

      //-------------------------user config CB profile related----------------

      val trigger_success_mail_flg_proflng = try {
        user_config.getString("dq_results_mail_config.trigger_success_mail_flg_dq")
      } catch {
        case _: Missing => "true" // If the flag is missing, set the default value as true i.e. it will trigger email
      }
      val trigger_success_mail_flg_histgrm = try {
        user_config.getString("dq_results_mail_config.trigger_success_mail_flg_dq")
      } catch {
        case _: Missing => "true" // If the flag is missing, set the default value as true i.e. it will trigger email
      }
      val save_failing_rows = try {
        user_config.getString("DQ_preferences.Save_Bad_Records")
      } catch {
        case _: Missing => "false"
      }
      log.info("save_failing_rows: " + save_failing_rows)
      //val save_failing_rows = user_config.getString("DQ_preferences.Save_Bad_Records")
      var constraints_Path = try {
        user_config.getString("DQ_preferences.DQ_Rules_File_".concat(env))
      } catch {
        case _: Missing => ""
      }
      log.info("constraints_Path" + constraints_Path)
      //val constraints_Path = user_config.getString("DQ_preferences.DQ_Rules_File")
      val preprocess_Path = try {
        user_config.getString("DQ_preferences.Preprocess_Rules")
      } catch {
        case _: Missing => ""
      }
      log.info("preprocess_Path" + preprocess_Path)
      val anomaly_records = try {
        user_config.getString("DQ_preferences.Historical_Records_".concat(env))
      } catch {
        case _: Missing => ""
      }

      //val anomaly_records = user_config.getString("DQ_preferences.Historical_Records")
      //val preprocess_Path = user_config.getString("DQ_preferences.Preprocess_Rules")
      val profiling_records = try {
        user_config.getString("DQ_preferences.Profiling_Records_".concat(env))
      } catch {
        case _: Missing => ""
      }
      log.info("profiling_records:" + profiling_records)
      //val profiling_records = user_config.getString("DQ_preferences.Profiling_records")
      val constraints_Path_bad_records = try {
        user_config.getString("DQ_preferences.DQ_Rules_Capture_Bad_Records")
      } catch {
        case _: Missing => ""
      }

      val Output_profiling_database = frmwrk_config.getString("Output_data.Output_profiling_database_".concat(env))
      val Output_profiling_table = frmwrk_config.getString("Output_data.Output_profiling_table")
      val historical_table = frmwrk_config.getString("Output_data.Historical_Table")
      val stability_index_table = {
        try {
          frmwrk_config.getString("Output_data.Stability_index_table")
        } catch {
          case e: Missing => ""
        }
      }

      // -- Need to add the staging table for profiling as well as insert staement needs to be added
      val Output_profiling_table_stg = try { frmwrk_config.getString("Output_data.Output_profiling_table_stg")
      } catch {
        case e: Missing => ""
      }

      val historical_table_stg = try {
        frmwrk_config.getString("Output_data.Historical_Table_stg")
      }  catch {
        case e: Missing => ""
      }
      val stability_index_table_stg = {
        try {
          frmwrk_config.getString("Output_data.Stability_index_table_stg")
        } catch {
          case e: Missing => ""
        }
      }

      var consolidatedSql = ""
      val failure_Threshold = {
        try {
          user_config.getString("Input_data.Failure_Threshold").toInt
        } catch {
          case e: ConfigException.Missing =>
            println("-----inside case ConfigException.Missing condition-----")
            10000
          //case _ =>
          //println("-----inside case else condition -----")
          //10000
        }
      }
      //-------------end of user configuration read---------------------------


      val dqChkConsOpPath = ""
      //val conf = new SparkConf()//.setAppName(jobApplicationName)
      //val sc = SparkContext.getOrCreate()
      //val spark = SparkSession.builder.appName(jobApplicationName).master("local[*]").getOrCreate()
      //SparkSession.builder.appName(SPARK_APP_NAME)
      for ((k, v) <- spark_args) {
        jobApplicationName = jobApplicationName.replaceAll("<" + k + ">", v)
        data_profile_attachment_names = data_profile_attachment_names.replaceAll("<" + k + ">", v)
        bad_records_attachment_names = bad_records_attachment_names.replaceAll("<" + k + ">", v)
        hist_trnd_anlys_attachment_names = hist_trnd_anlys_attachment_names.replaceAll("<" + k + ">", v)
        hdfs_path_for_dq_result = hdfs_path_for_dq_result.replaceAll("<" + k + ">", v)
        err_mail_sub = err_mail_sub.replaceAll("<" + k + ">", v)
        err_mail_body = err_mail_body.replaceAll("<" + k + ">", v)
      }

      err_mail_body = err_mail_body  + " " +  indicator_str
      val spark = SparkSession.builder.appName(jobApplicationName)
        .config("spark.sql.autoBroadcastJoinThreshold", "40085760")
        //.config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped","true")
        //.config("org.apache.hadoop.mapred.FileOutputCommitter.class", "com.aaa.dq.services.DQFileOutputCommitter")
        //.config("org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol.class", "com.aaa.dq.services.SQLHadoopMapReduceCommitProtocol")
        //.config("org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol.class", "com.aaa.dq.services.SQLHadoopMapReduceCommitProtocol")
        //.setOutputCommitter(classOf[DQFileOutputCommitter])
        .enableHiveSupport()
        .getOrCreate()
      val appId = spark.sparkContext.applicationId
      val sc = spark.sparkContext
      //sc.setLogLevel("DEBUG")
      // fileoutput committer
      // update temporary path for committer, to be in the same volume
      val conf: JobConf = new JobConf(sc.hadoopConfiguration)

      System.out.println("Outputcommitter: "+conf.getOutputCommitter)
      System.out.println("hive.exec.stagingdir: "+conf.get("hive.exec.stagingdir"))
      System.out.println("hive.exec.scratchdir: "+conf.get("hive.exec.scratchdir"))
      //conf.set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped","true")
      //conf.setOutputCommitter(classOf[DQFileOutputCommitter])
      //conf.setOutputCommitter(org.apache.hadoop.mapreduce.lib.output.OutputCommitter.class)
      //com.aaa.dq.services.FileOutputCommitter.PENDING_DIR_NAME_Recevd = "_temporary_"+appId
      errMailGlblConfig += ("err_mail_sub" -> err_mail_sub, "err_mail_body" -> err_mail_body, "dq_error_sent_to" -> dq_error_sent_to, "env" -> env, "run_date" -> DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now), "jobApplicationName" -> jobApplicationName, "spark_log_path" -> spark_log_path.replace("<app_id>", appId), "dataset_name" -> dataset_name)
      println("Error configuration map values. errMailGlblConfig: " + errMailGlblConfig)

      //append spark args into mail configuration
      //errMailGlblConfig = errMailGlblConfig.++(spark_args);
      var argsList: List[Array[String]] = List[Array[String]]()
      val table_string = if (formatType == "hive") {
        val filename = dq_IpHiveQuery.split("/").last
        //consolidatedSql = Source.fromFile(filename).getLines.mkString.replace("<process_date>",process_date)
        try {
          //Read the user provided sql file
          consolidatedSql = Source.fromFile(filename).getLines.mkString
        }
        catch {
          case e: Exception =>
            //In case if user didn't provide the sql file, read directly from user config file
            consolidatedSql = dq_IpHiveQuery
        }
        //Substitute the "<tags>" in sql with the key value pair provide in spark submit arguments
        for ((k, v) <- spark_args) consolidatedSql = consolidatedSql.replaceAll("<" + k + ">", v)
        val logicalPlan = spark.sessionState.sqlParser.parsePlan(consolidatedSql)
        import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
        val tables = logicalPlan.collect { case r: UnresolvedRelation => r.tableName }
        tables.mkString(",")
      } else {
        "NA"
      }
      //for(ip_file <- INPUT_FILE.asScala.toList) {
      var qcArgs: Array[String] = null
      qcArgs = Array("input_file", ip_file,
        "appName", jobApplicationName,
        "format", formatType,
        "ipSchemaPath", ipSchemaPath,
        "dq_IpHiveQuery", dq_IpHiveQuery,
        "table_string", table_string,
        "hiveDB", hiveDB,
        "Outputhive_table", Outputhive_table,
        "dqChkConsOpPath", dqChkConsOpPath,
        "save_failing_rows", save_failing_rows,
        "val_sep", val_sep,
        "val_header", val_header,
        "val_infr_schema", val_infr_schema,
        "val_null_vl", val_null_vl,
        "val_charset", val_charset,
        "appId", appId,
        "process_date", process_date,
        "preprocess_Path", preprocess_Path,
        "anomaly_records", anomaly_records,
        "profiling_records", profiling_records,
        "historical_table", historical_table,
        "stability_index_table", stability_index_table,
        "Output_profiling_database", Output_profiling_database,
        "Output_profiling_table", Output_profiling_table,
        "data_profile_attachment_names", data_profile_attachment_names,
        "bad_records_attachment_names", bad_records_attachment_names,
        "hist_trnd_anlys_attachment_names", hist_trnd_anlys_attachment_names,
        "dataset_name",dataset_name,
        "multi_line_flg",multi_line_flg,
        "Dataset_Multi_Line_quote",Dataset_Multi_Line_quote,
        "Dataset_Multi_Line_escape",Dataset_Multi_Line_escape,
        "multi_line_flg",multi_line_flg,
        "historical_table_stg", historical_table_stg,
        "stability_index_table_stg", stability_index_table_stg,
        "Output_profiling_table_stg", Output_profiling_table_stg
      )

      argsList = qcArgs :: argsList
      //}
      //Invoke log4jSetConfig method and passing appId as a Parameter to the log4j property file for writing dynamic log file for each jobs.
      //ConfigParser.log4jSetConfig(jobApplicationName, appId, log4jPropPath, basePath)

      val appender = new RollingFileAppender()
      appender.setAppend(true)
      appender.setMaxFileSize("2MB")
      appender.setMaxBackupIndex(1)
      appender.setFile("DQ_" + appId + "_" + DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now) + ".log")
      appender.activateOptions()

      val layOut = new PatternLayout()
      layOut.setConversionPattern("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n")
      appender.setLayout(layOut)
      log.addAppender(appender)
      log.info("Job Start Time is -----> " + startTimeStamp)
      log.info("---------- Spark session successfully initialized ---------")
      log.info("---------- Data Check Quality process started ----------")

      //PropertyConfigurator.configure("log4j.properties")
      //Application Name and Application ID
      val jobNameAppId = jobApplicationName + "_" + appId
      //{appname}.{appid}.{ts}.log
      log.info("The Data Check Quality rules input path --> " + constraints_Path)
      log.info("The input file path --> " + ip_file)
      log.info("The success data quality output file path --> " + dqChkConsOpPath)
      //log4jSetConfig(jobApplicationName, appId, log4jPropPath, basePath)

      var pool = 0
      def poolId = {
        pool = pool + 1
        pool
      }

      //-------------- Populate details for dq results email ------------------------
      var input_sql_or_file = ""
      if (formatType == "hive") {
        input_sql_or_file = consolidatedSql
      }
      else {
        input_sql_or_file = ip_file
      }

      mail_config += ("mail_sub" -> (exec_mode match {
        case "dq" => dq_rule_mail_sub
        case "profile" => data_profile_mail_sub
        case "trnd_anlys" | "review" | "review_psi" => hist_trnd_anlys_mail_sub
        case "bad_records" => hist_trnd_anlys_mail_sub
      }),
        "mail_body" -> (exec_mode match {
          case "dq" => dq_rule_mail_body
          case "profile" => data_profile_mail_body
          case "trnd_anlys" | "review" | "review_psi" => hist_trnd_anlys_mail_body
          case "bad_records" => hist_trnd_anlys_mail_sub
        }),
        "jobApplicationName" -> jobApplicationName,
        "appId" -> appId,
        "run_date" -> run_date,
        "dataset_name" -> dataset_name,
        "jobStatus" -> "",
        "env" -> env,
        "dq_results_sent_to" -> dq_results_sent_to,
        "dq_error_sent_to" -> dq_error_sent_to,
        "hdfs_path_for_dq_result" -> hdfs_path_for_dq_result,
        "spark_log_path" -> spark_log_path.replace("<app_id>", appId),
        "err_mail_sub" -> err_mail_sub,
        "err_mail_body" -> err_mail_body,
        "attachment_names" -> (exec_mode match {
          case "dq" => dataset_name
          case "profile" => data_profile_attachment_names
          case "trnd_anlys" => hist_trnd_anlys_attachment_names // -- set this value inside trnd analysis section
          case "review" | "review_psi" => "" // -- set this value inside trnd analysis section
          case "bad_records" => bad_records_attachment_names // -- set this value inside trnd analysis section
        }),
        "jobStatus" -> rule_status,
        "dq_error_sent_to" -> dq_error_sent_to
      )

      //--------append spark args into mail configuration
      mail_config = mail_config.++(spark_args)

      def runner(qcArgsThread: Array[String]) = Future {
        try {
          sc.setLocalProperty("spark.scheduler.pool", poolId.toString)
          log.info("Arguments for Data Quality Chk Thread" + qcArgsThread)
          log.info("constraints_Path: " + constraints_Path)
          //--------------Dq------------------
          if (exec_mode == "dq") {
            var json_content = ConfigParser.parse(constraints_Path, "True")
            for ((k, v) <- spark_args) json_content = json_content.replaceAll("<" + k + ">", v)
            /*          val config_rules = JSON.parseFull(json_content).getOrElse(null).asInstanceOf[Map[String, Any]]
                        println(config_rules)
                        log.info("The config_rules --> " + config_rules)
                        val rules = config_rules("rules").asInstanceOf[List[Map[String, Any]]]
                        val checks = BuildChecks.build(rules, log)
            */


            //Single rule file for multiple table
            val dq_rules = List(json_content)
            import spark.implicits._
            val dq_rules_df = dq_rules.toDF("table_rules")
            val maxJsonArray = 1000
            val jsonEntities = (0 until maxJsonArray).map(jsonData => get_json_object($"table_rules", s"$$[$jsonData]"))
            val splittedRulesRowsDF = dq_rules_df.withColumn("splitted_rules", explode(array(jsonEntities: _*)))
              .where(!isnull($"splitted_rules")).withColumn("hive_table", json_tuple(col("splitted_rules"), "entity_name")).drop("table_rules")
            splittedRulesRowsDF.show(false)

            var itr = 0
            val ruleRow = splittedRulesRowsDF.count()
            val ruleSet = splittedRulesRowsDF.collectAsList()

            //while(itr <= ruleRow) {
            do {
              val rule_content = ruleSet.get(itr).getString(0)
              val HiveTableName = ruleSet.get(itr).getString(1)

              println("rule_content : " + rule_content)
              val config_rules = JSON.parseFull(rule_content).getOrElse(null).asInstanceOf[Map[String, Any]]
              println("DQ config_rules : " + config_rules)
              log.info("The config_rules --> " + config_rules)
              val rules = config_rules("rules").asInstanceOf[List[Map[String, Any]]]
              println("rule : " + rules)

              val checks = BuildChecks.build(rules, log)
              log.info("The Data Quality checks are-------> " + checks)

              val retValQcReport = Report_thread.getQC_report(qcArgsThread, checks, spark, sc, log, spark_args, HiveTableName)

              if (retValQcReport == "noRecordsFound") {
                //No records found to process DQ. gracefully exit spark job.
                log.info("No records found to process DQ. exit spark job.")
                println("No records found to process DQ. exit spark job.")

                if (kill_job_for_zero_input_records_flg_dq == "true".toLowerCase) { //Kill the Spark/Autosys job
                  throw new RuntimeException("There is no input data available to perform DQ. Exiting the script. Please check the source table or input file for data availability")
                  //System.exit(1)
                }
                else {

                  val err_msg = "There is no input data available to perform DQ. Exiting the script. Please check the source table or input file for data availability"
                  //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
                  constraints_Path = null // setting this null value will force exit this "def runner" method and spark job will exit gracefully
                }
                /*
              sys.ShutdownHookThread
              sc.stop()
              spark.stop()
              spark.yarn.app */
              }

              //-------validate failure flag to kill the job based on user provided threshold

              if (constraints_Path != null && !constraints_Path.isEmpty()) {
                println("---------------Before failure threshold calculation------------------")
                val df = spark.sql("select * from dataQualityResultDF")
                df.show(truncate = false)
                val threshold_Error_Count = df.filter(df("constraint_status") === "Failure" && df("Failure_Flag").rlike("Error")).count()
                var threshold_status = ""

                println("--------------- Threshold flag failed constraint count------" + threshold_Error_Count)

                val failed_constraints_cnt = Report_thread.failed_constraints_cnt
                val succeeded_constraints_cnt = Report_thread.succeeded_constraints_cnt
                val tot_row_count = Report_thread.count_df.toString()

                if (failed_constraints_cnt > 0) {
                  rule_status = "FAILED"
                }
                else {
                  rule_status = "SUCCESS"
                }

                if (threshold_Error_Count >= failure_Threshold.toInt) {
                  log.error("Data Quality failure has reached the user provided threshold (Failure_Flag value in user configuration file). Exiting the job")
                  println("Data Quality failure has reached the user provided threshold (Failure_Flag value in user configuration file). Exiting the job")

                  //Inserting dq_status_partn table into HIVE table
                  val endTimeStamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss").format(LocalDateTime.now)
                  val jobEndTime = endTimeStamp.toString

                  log.info("Job End Time is -----> " + jobEndTime)

                  threshold_status = "FAILED"
                  val data = spark.createDataFrame(Seq((appId, jobApplicationName, jobStartTime, jobEndTime, Outputhive_table, threshold_status, formatType, dataset_name, current_Date)))
                    .toDF("job_run_id", "application_name", "start_time", "end_time", "job_description", "status", "input_data_type", "Dataset_name", "currentdate")
                    .withColumn("start_time", lit(from_unixtime(unix_timestamp(lit(jobStartTime), "yyyy-MM-dd_hhmmss"))))
                    .withColumn("end_time", lit(from_unixtime(unix_timestamp(lit(jobEndTime), "yyyy-MM-dd_hhmmss"))))
                    .withColumn("currentdate", lit(from_unixtime(unix_timestamp(lit(current_Date), "yyyy-MM-dd"))))
                  log.info("The schema for " + hiveDB + "." + Outputhive_table + " " + data.printSchema())
                  data.show(false)

                  //hiveInsert(data: DataFrame, spark: SparkSession, sc: SparkContext, hiveDeequStatDb: String, hiveDeequStatTbl: String)

                  log.info("after hiveInsert function call")
                } // end of if part -- if(threshold_Error_Count >= failure_Threshold.toInt)
                else {

                  //Inserting Deequ status into HIVE table
                  val endTimeStamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss").format(LocalDateTime.now)
                  val jobEndTime = endTimeStamp.toString
                  log.info("Job End Time is -----> " + jobEndTime)

                  threshold_status = "SUCCESS"

                  val data = spark.createDataFrame(Seq((appId, jobApplicationName, jobStartTime, jobEndTime, Outputhive_table, threshold_status, formatType, dataset_name, current_Date)))
                    .toDF("job_run_id", "application_name", "start_time", "end_time", "job_description", "status", "input_data_type", "Dataset_name", "currentdate")
                    .withColumn("start_time", lit(from_unixtime(unix_timestamp(lit(jobStartTime), "yyyy-MM-dd_hhmmss"))))
                    .withColumn("end_time", lit(from_unixtime(unix_timestamp(lit(jobEndTime), "yyyy-MM-dd_hhmmss"))))
                    .withColumn("currentdate", lit(from_unixtime(unix_timestamp(lit(current_Date), "yyyy-MM-dd"))))
                  log.info("The schema for " + hiveDB + "." + Outputhive_table + " " + data.printSchema())
                  data.show(false)

                  //data.write.mode("append").insertInto(hiveDeequStatDb + "." + hiveDeequStatTbl)
                  //hiveInsert(data: DataFrame, spark: SparkSession, sc: SparkContext, hiveDeequStatDb: String, hiveDeequStatTbl: String)
                  log.info("The Output Data profiling  successfully writing in to  -----> " + hiveDB + "." + Outputhive_table)
                } // end of else part -- if(threshold_Error_Count >= failure_Threshold.toInt)

                System.out.println("~~~~~~~current user~~~~~~~~~~~ " + System.getProperty("user.name"))

                //tempTable registered in ConstraintGlobal.scala
              /*
                val dq_report = spark.sql("select validation_rule as Data_Quality_Rule, resulting_status as Result, " +
                "total_rows as Total_Rows, pass_count as Pass_Count, fail_count as Fail_Count, " +
                "constraint_msg as Constraint_Message from tempTable")
              */

                var dq_report: DataFrame = null
                if (formatType == "hive")
                  dq_report = spark.sql("select hive_dataset as Hive_Dataset, column_name as Column_Name, rule_name as Rule_Name, dq_rule_description as Data_Quality_Rule, resulting_status as Result, total_rows as Total_Rows, get_pass_count as Pass_Count, get_fail_count as Fail_Count, Pass_percentage as Pass_Percentage, Fail_percentage as Fail_Percentage, constraint_messages as Constraint_Message, Constraint_rule as Constraint_Rule FROM tempTable")
                else
                  dq_report = spark.sql("select column_name as Column_Name, rule_name as Rule_Name, dq_rule_description as Data_Quality_Rule, resulting_status as Result, total_rows as Total_Rows, get_pass_count as Pass_Count, get_fail_count as Fail_Count, Fail_percentage as Fail_Percentage, constraint_messages as Constraint_Message, Constraint_rule as Constraint_Rule from tempTable")

                dq_report.show(10, false)

                dq_report
                  .coalesce(1)
                  .write
                  .mode("append")
                  .option("header", "true")
                  .csv(hdfs_path_for_dq_result)

                log.info("---------------- before sending DQ email ----------------")

                mail_config += ("jobStatus" -> rule_status, "input_sql_or_file" -> input_sql_or_file, "failed_constraints_cnt" -> failed_constraints_cnt.toString(), "succeeded_constraints_cnt" -> succeeded_constraints_cnt.toString(), "tot_row_count" -> tot_row_count)
                log.info("failed_constraints_cnt:" + failed_constraints_cnt + " failure_Threshold:" + failure_Threshold)

                if (threshold_Error_Count >= failure_Threshold.toInt) {
                  log.info("-------------- Inside failed_constraints_cnt DQ email ----------------")

                  //sndEmail.sendReport(mail_config.toMap.asJava)
                  log.info("failed mandatory constraints count:" + threshold_Error_Count + " failure_Threshold:" + failure_Threshold)

                  val dq_err_msg = "\n\n Data Quality failure has reached the user provided threshold (Failure_Flag value in user configuration file). Exiting the job"
                  //sndEmail.errorMail(errMailGlblConfig.asJava, dq_err_msg)
                  System.exit(1)
                }

                if (trigger_success_mail_flg_dq == "true".toLowerCase) {
                  //sndEmail.sendReport(mail_config.toMap.asJava)
                  log.info("-------------- after sending DQ email ----------------")
                }
                  else {
                  if (failed_constraints_cnt > 0) {
                    log.info("----Found some rule failures, overriding failed_constraints_cnt flag and triggering the mail. Failure Count: " + failed_constraints_cnt)
                    //sndEmail.sendReport(mail_config.toMap.asJava)
                  }
                  else {
                    log.info("-------------- Not triggering DQ email as trigger_success_mail_flg_dq flag not set to true ----------------")
                  }
                }
              }

              spark.catalog.dropTempView("dataQualityResultDF")
              spark.catalog.dropTempView("tempTable")
              itr = itr + 1
            }

            while (itr < ruleRow)
          }


          //-------------------------------------------------profiling--------------------------------------------

          if (exec_mode == "profile") {

            val profiling_str = ConfigParser.parse(profiling_records, "True")
            val profiling_data = JSON.parseFull(profiling_str).getOrElse(null).asInstanceOf[Map[String, Any]]
            val metrics_rules = profiling_data("rules").asInstanceOf[List[Map[String, Any]]]
            log.info("metrics_rules:" + metrics_rules)
            Report_thread.getmetrics_report(qcArgsThread, metrics_rules, spark, sc, log, spark_args)
            log.info("The Output Data profiling  successfully inserted in to  -----> " + hiveDB + "." + Outputhive_table)
            if (trigger_success_mail_flg_proflng == "true".toLowerCase) {
              log.info("---------------- before sending profile email ----------------")
              //sndEmail.sendReport(mail_config.toMap.asJava)
              log.info("---------------- after sending profile email ----------------")
            }
            else {
              log.info("-------------- Not triggering profiling email as trigger_success_mail_flg_proflng flag not set to true ----------------")
            }
          }

          //-------------------------------History trend analysis or Anomaly detection------------------------------
          if (exec_mode == "trnd_anlys") {

            log.info("Inside History Trend analysis section")
            log.info("Anamoly Records: " + anomaly_records)
            val anomaly_str = ConfigParser.parse(anomaly_records, "True")
            val anomaly_data = JSON.parseFull(anomaly_str).getOrElse(null).asInstanceOf[Map[String, Any]]
            log.info("anomaly_data:   " + anomaly_data)
            val anomaly_rules = anomaly_data("rules").asInstanceOf[List[Map[String, Any]]]
            Report_thread.processHistoricaldata(qcArgsThread, anomaly_rules, spark, sc, log, spark_args)

            //--------Failure check calculation
            //val df_trnd_anlys = spark.sql("select * from tmpTbl_dqhistoryanalysis")
            //val trnd_anlys_Error_Count = df_trnd_anlys.filter(df_trnd_anlys("Status") === "FAILURE").count()
            val trnd_anlys_Error_Count = process_historicaltrend.trnd_anlys_Error_Count
            if (trnd_anlys_Error_Count > 0) {
              rule_status = "FAILED" //This variable will later get passed to mail variables.
            }
            else {
              rule_status = "SUCCESS"
            }
            //----Update mail configuration
            mail_config += ("attachment_names" -> process_historicaltrend.filepath_list.distinct.mkString(","),
              "jobStatus" -> rule_status)
            log.info("-----------------------History trend attachment names "+ process_historicaltrend.filepath_list)
            if (trnd_anlys_Error_Count >= failure_Threshold.toInt) {
              log.info("---------------- one or more trend analysis failed. Number of failed rules: " + trnd_anlys_Error_Count +
                "Before sending trnd_anlys email ----------------")
              val dq_err_msg = "\n\n History trend analysis job has reached the user provided threshold. Exiting the job\n Number of failed rules: " + trnd_anlys_Error_Count
              val mail_body_err = hist_trnd_anlys_mail_body + "\n\n" + err_mail_body + "\n\n" + dq_err_msg
              mail_config += ("mail_body" ->  mail_body_err.toString )
              //sndEmail.sendReport(mail_config.toMap.asJava)
              println(err_mail_body)
              //sndEmail.errorMail(errMailGlblConfig.asJava, dq_err_msg)
              System.exit(1)
            }

            if (trigger_success_mail_flg_histgrm == "true".toLowerCase) {
              log.info("---------------- before sending trnd_anlys email ----------------")
              //sndEmail.sendReport(mail_config.toMap.asJava)
              log.info("---------------- after sending trnd_anlys email ----------------")
            }
          } // end of if (exec_mode == "trnd_anlys")

          if (exec_mode == "bad_records") {
            val HiveTableName = "" // will get it from rule_json_file
            log.info("Inside bad_records section")
            // ------- script related to seggregating bad records -----------
            var config_rules_content = ConfigParser.parse(constraints_Path_bad_records, save_failing_rows)
            for ((k, v) <- spark_args) config_rules_content = config_rules_content.replaceAll("<" + k + ">", v)
            val config_rules_rows = JSON.parseFull(config_rules_content).getOrElse(null).asInstanceOf[Map[String, Any]]
            val rules_rows = config_rules_rows("rules").asInstanceOf[Map[String, Any]]
            val rowCheckers = BuildRowChecker.build(rules_rows)

            //bad_records.process_recordcheck(qcArgsThread, rowCheckers,spark, sc, log, spark_args, HiveTableName )
            bad_records.process_recordcheck(qcArgsThread, rowCheckers,spark, sc, log, spark_args)
            //--------Failure check calculation
            //val df_trnd_anlys = spark.sql("select * from tmpTbl_dqhistoryanalysis")
            //val trnd_anlys_Error_Count = df_trnd_anlys.filter(df_trnd_anlys("Status") === "FAILURE").count()
            val trnd_anlys_Error_Count = process_historicaltrend.trnd_anlys_Error_Count
            if (trnd_anlys_Error_Count > 0) {
              rule_status = "FAILED" //This variable will later get passed to mail variables.
            }
            else {
              rule_status = "SUCCESS"
            }

            //----Update mail configuration

            mail_config += ("attachment_names" -> "", "jobStatus" -> rule_status)
            log.info("-----------------------History trend attachment names "+ process_historicaltrend.filepath_list)

            if (trnd_anlys_Error_Count >= failure_Threshold.toInt) {

              log.info("---------------- one or more trend analysis failed. Number of failed rules: " + trnd_anlys_Error_Count + "Before sending trnd_anlys email ----------------")
              //sndEmail.sendReport(mail_config.toMap.asJava)
              val dq_err_msg = "\n\n History trend analysis job has reached the user provided threshold. Exiting the job\n Number of failed rules: " + trnd_anlys_Error_Count
              //sndEmail.errorMail(errMailGlblConfig.asJava, dq_err_msg)
              System.exit(1)
            }

            log.info("---------------- before sending bad_records email ----------------")
            //sndEmail.sendReport(mail_config.toMap.asJava)
            log.info("---------------- after sending bad_records email ----------------")
          }


          if (exec_mode == "review" || exec_mode == "review_psi") {
            log.info("Inside review analysis section")

            review_records.process_thresholdcheck(qcArgsThread, spark, sc, log, spark_args,exec_mode)


            //--------Failure check calculation
            //val df_trnd_anlys = spark.sql("select * from tmpTbl_dqhistoryanalysis")
            //val trnd_anlys_Error_Count = df_trnd_anlys.filter(df_trnd_anlys("Status") === "FAILURE").count()

            val trnd_anlys_Error_Count = process_historicaltrend.trnd_anlys_Error_Count
            if (trnd_anlys_Error_Count > 0) {
              rule_status = "FAILED" //This variable will later get passed to mail variables.
            }
            else {
              rule_status = "SUCCESS"
            }

            //----Update mail configuration
            mail_config += ("attachment_names" -> "", "jobStatus" -> rule_status)
            log.info("-----------------------History trend attachment names "+ process_historicaltrend.filepath_list)
            if (trnd_anlys_Error_Count >= failure_Threshold.toInt) {
              log.info("---------------- one or more trend analysis failed. Number of failed rules: " + trnd_anlys_Error_Count + "Before sending trnd_anlys email ----------------")
              //sndEmail.sendReport(mail_config.toMap.asJava)
              val dq_err_msg = "\n\n History trend analysis job has reached the user provided threshold. Exiting the job\n Number of failed rules: " + trnd_anlys_Error_Count
              //sndEmail.errorMail(errMailGlblConfig.asJava, dq_err_msg)
              System.exit(1)
            }

           if (trigger_success_mail_flg_histgrm == "true".toLowerCase) {
              log.info("---------------- before sending trnd_anlys email ----------------")
              //sndEmail.sendReport(mail_config.toMap.asJava)
              log.info("---------------- after sending trnd_anlys email ----------------")
            }
          } // end of if (exec_mode == "trnd_anlys")
          // println(qcArgsThread.mkString(" "))
        } // end of try statement in def runner

        catch {
          case e: Exception =>
            log.error("Issue in processing the Data Quality")
            System.out.println("Failure happened!")
            log.error(e.getMessage)
            log.error(e.printStackTrace())
            var err_msg = e.getMessage
            val status = "FAILURE"
            //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
            System.exit(1)
        }
      }//-- end of def runner

      val futures = argsList map (i => runner(i))
      log.info("---------- Data check quality constraint process successfully completed ----------")
      println("---------- Data check quality constraint process successfully completed ----------")

      // now you need to wait all your futures to be completed

      val allFutures = Future.sequence(futures)
      val result =
        try {
          Await.result(allFutures, Duration.Inf)
        } catch {
          case e: Exception =>
            System.err.println("Failure happened!")
            log.error(e.getMessage)
            log.error(e.printStackTrace())
            System.exit(1)
        }

      //log.info("---------- Successfully wrote data into the dq constraints Hive table ----------")
      //println("---------- Successfully wrote data into the dq constraints Hive table ----------")
    } // end of try statement in def main
    catch {
      case e: Exception =>
        log.error("Issue in processing the Data Quality")
        System.out.println("Failure happened!")
        log.error(e.getMessage)
        log.error(e.printStackTrace())
        val err_msg = e.getMessage
        val status = "FAILURE"
        //sndEmail.errorMail(errMailGlblConfig.asJava, err_msg)
        System.exit(1)

    }
  }
}


