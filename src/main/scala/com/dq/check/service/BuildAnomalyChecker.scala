package com.dq.check.service

import java.time.YearMonth
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map
import scala.util.control.NonFatal

object BuildAnomalyChecker {
  /**
   * @return mixed
   */
  def build(rules: List[Map[String, Any]], db_parameters: Map[String,String], kvpairs: Map[String, String], spark: SparkSession, sc: SparkContext,log: Logger) = {

    println(rules)
    var df_final = Seq[DataFrame]()
    var df_currdata = Seq[DataFrame]()
    val db = kvpairs("Output_profiling_database")
    val table = kvpairs("Output_profiling_table")
    var threshold_chk_seq = Seq[DataFrame]()
    var gen_sql_strtgy_seq = Seq[DataFrame]()

    //val multiplier = (x1: String, x2: Seq[String]) => x2.map(_ + "_" + x1)
    try {
      //Loop through json elements
      for (qc_constraint <- rules) {
        log.info("qc_constraint" + qc_constraint)
        val name = qc_constraint("name")
        val args = qc_constraint("args")
        val report_details = qc_constraint.getOrElse("report_details",List("csv","trend_report")).asInstanceOf[List[String]]
        val sheet_name = qc_constraint.getOrElse("sheet_name",List("sheet1","sheet2","sheet3","sheet4")).asInstanceOf[List[String]]

        val arg = args.asInstanceOf[Map[String, Any]]

        //val cycle_monthNow = cycle_month.format(DateTimeFormatter.ofPattern("yyyy.MM"))
        val format = DateTimeFormatter.ofPattern("yyyyMM")

        name match {
          case "psi" => {
            val col_list = arg.getOrElse("column", List("test")).asInstanceOf[List[String]]

            val curr_data_where = arg.getOrElse("current_data", " ").asInstanceOf[String]
            val hist_data_where = arg.getOrElse("Historical_data", " ").asInstanceOf[String]

            val lowerbound = arg.get("lowerbound").asInstanceOf[Option[Double]].get
            val upperbound = arg.get("upperbound").asInstanceOf[Option[Double]].get
            val value = arg.get("value").asInstanceOf[Option[String]].get
            val time_period = arg.get("time_period").asInstanceOf[Option[String]].get


            var curr_sql = "select column_name, metric_name," + value + " as value from <db.table> where column_name IN (<col_list>) and " + curr_data_where
            for ((k,v) <- db_parameters) curr_sql = curr_sql.replaceAll("<"+k+">",v)
            curr_sql = curr_sql.replaceAll("<db.table>", db + "." + table)

            var hist_sql = if (hist_data_where.toLowerCase().startsWith("select")){
              hist_data_where}
            else{
              "select column_name, metric_name," + value + " as value," + time_period + " as time_period from <db.table> where column_name IN (<col_list>) and " + hist_data_where}
            for ((k,v) <- db_parameters) hist_sql = hist_sql.replaceAll("<"+k+">",v)
            hist_sql = hist_sql.replaceAll("<db.table>", db + "." + table)
            curr_sql = curr_sql.replaceAll("<db>", db)
            hist_sql = hist_sql.replaceAll("<db>", db)

            curr_sql = curr_sql.replaceAll("<col_list>", col_list.mkString("'", "', '", "'"))
            hist_sql = hist_sql.replaceAll("<col_list>", col_list.mkString("'", "', '", "'"))

            log.info("curr_query for PSI " + curr_sql)
            log.info("hist_query for PSI " + hist_sql)

            val df_hist = spark.sql(hist_sql)
            val df_curr = spark.sql(curr_sql)

            process_historicaltrend.process_psitrend(List(df_hist, df_curr),kvpairs,db_parameters,report_details,sheet_name,spark,sc,lowerbound,upperbound)
          }
          case "threshold_check" => {
            println("args for threshold: " + arg)
            log.info("args for threshold: " + arg)
            val metric_name = arg.getOrElse("metric_name", "").asInstanceOf[List[String]].mkString("'", "', '", "'")
            var col_list1 = arg.getOrElse("column", List("test"))
            println("col_list: " + col_list1)
            log.info("col_list: " + col_list1)
            val col_list = arg.getOrElse("column", List("test")).asInstanceOf[List[String]]
            val value = arg.get("value").asInstanceOf[Option[String]].get
            val lowerbound = arg.get("lowerbound").asInstanceOf[Option[String]].get
            val upperbound = arg.get("upperbound").asInstanceOf[Option[String]].get

            val curr_data_where = arg.get("current_data").asInstanceOf[Option[String]].get
            val Historical_data_where = arg.get("Historical_data").asInstanceOf[Option[String]].get

            var curr_sql = "select column_name, metric_name," + value + " as value from <db.table> where column_name IN (<col_list>) and metric_name IN (" + metric_name + ") and " + curr_data_where
            for ((k,v) <- db_parameters) curr_sql = curr_sql.replaceAll("<"+k+">",v)
            curr_sql = curr_sql.replaceAll("<db.table>", db + "." + table)

            var hist_sql = "select column_name, metric_name," + lowerbound + " as lowerbound," + upperbound + " as upperbound from <db.table> where column_name IN (<col_list>) and metric_name IN (" + metric_name + ") and " + Historical_data_where + " group by column_name, metric_name"
            for ((k,v) <- db_parameters) hist_sql = hist_sql.replaceAll("<"+k+">",v)
            hist_sql = hist_sql.replaceAll("<db.table>", db + "." + table)
            curr_sql = curr_sql.replaceAll("<db>", db)
            hist_sql = hist_sql.replaceAll("<db>", db)

            curr_sql = curr_sql.replaceAll("<col_list>", col_list.mkString("'", "', '", "'"))
            hist_sql = hist_sql.replaceAll("<col_list>", col_list.mkString("'", "', '", "'"))

            log.info("curr_query for threshold " + curr_sql)
            log.info("hist_query for threshold " + hist_sql)

            val df_hist = spark.sql(hist_sql)
            val df_curr = spark.sql(curr_sql)
            //Generate a sequence of dataframe
            threshold_chk_seq = threshold_chk_seq :+ process_historicaltrend.process_thresholdcheck(List(df_hist, df_curr),kvpairs,db_parameters,report_details,sheet_name,spark,sc)
          }

          case "generic_sql_strategy" => {
            val data_sql = arg.getOrElse("Historical_data", " ").asInstanceOf[String]
            val col_list = arg.getOrElse("column", List("test")).asInstanceOf[List[String]]

            var sql_query = data_sql
            for ((k,v) <- db_parameters) sql_query = sql_query.replaceAll("<"+k+">",v)
            sql_query = sql_query.replaceAll("<db.table>", db + "." + table)

            var currdf_sql = arg.getOrElse("Current_data", " ").asInstanceOf[String]
            for ((k,v) <- db_parameters) currdf_sql = currdf_sql.replaceAll("<"+k+">",v)
            currdf_sql = currdf_sql.replaceAll("<db.table>", db + "." + table)
            sql_query = sql_query.replaceAll("<db>", db)
            currdf_sql = currdf_sql.replaceAll("<db>", db)

            sql_query = sql_query.replaceAll("<col_list>", col_list.mkString("'", "', '", "'"))
            currdf_sql = currdf_sql.replaceAll("<col_list>", col_list.mkString("'", "', '", "'"))

            log.info("sql_query for mean " + sql_query)
            log.info("sql_query for mean " + currdf_sql)

            val df_hist = spark.sql(sql_query)
            val df_curr = spark.sql(currdf_sql)
            gen_sql_strtgy_seq = gen_sql_strtgy_seq :+ process_historicaltrend.process_generictrend(List(df_hist, df_curr),kvpairs,db_parameters,report_details,sheet_name,spark,sc)
          }
        }
      } //end of for loop
      //consolidate the df sequence.
      if(!threshold_chk_seq.isEmpty){
        val df_consolidated_threshold_chk:DataFrame = threshold_chk_seq.reduce(_ union _)
        df_consolidated_threshold_chk
          .coalesce(1)
          .write.mode("overwrite")
          .format("csv")
          .option("emptyValue", null)
          .option("nullValue", null)
          .option("header", "true")
          .save(process_historicaltrend.filePath_mean_report)
      }

      if(!gen_sql_strtgy_seq.isEmpty){
        //Frequency report
        val df_consolidated_gen_sql_strtgy:DataFrame = gen_sql_strtgy_seq.reduce(_ union _)
        df_consolidated_gen_sql_strtgy
          .coalesce(1)
          .write.mode("overwrite")
          .format("csv")
          .option("emptyValue", null)
          .option("nullValue", null)
          .option("header", "true")
          .save(process_historicaltrend.filePath_freq_report)
      }

    }
    catch {
      case NonFatal(e) =>
        System.err.println("Failure happened in BuildAnomalyChecker routine")
        System.err.println(e.getMessage)
        println(e.printStackTrace())
        System.exit(1)
    }
  }
}
