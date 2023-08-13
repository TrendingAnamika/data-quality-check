package com.dq.check.service

import java.io.IOException
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.schema._
//import com.aaa.dq.services.DataCheckImplementation.{errMailGlblConfig, sndEmail}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.control.NonFatal
import scala.util.matching.Regex
import org.apache.log4j.Logger
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import transformation_util.{correctConsMsg, getColumnNameUdf, getDQRule, getDQRuleUdf, getFailCount, getFinalDQRuleDescription, getFinalRule, getPassCount, getRuleNameUdf}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Map
import scala.collection.JavaConverters._
import scala.io.Source
import org.apache.spark.sql.functions.{col, udf}
import java.security.MessageDigest
import java.math.BigInteger



/**
 * Created By Anamika Singh
 * @global
 */



class QualityCheckerGlobal{

  def do_check(data_df :org.apache.spark.sql.DataFrame, checks: Check): com.amazon.deequ.VerificationResult = {
    println("--------------before starting dq. This is source data frame---------")
    data_df.show(30,truncate=false)
    println("--------------before starting dq. This is check value---------")
    println(checks.toString())

    val verificationResult = VerificationSuite()
      .onData(data_df)
      .addCheck(checks)
      .run()

    println(verificationResult.metrics)
    println("--------------Processed Results-----------------")
    println(verificationResult.toString())

    return  verificationResult
  }

}



object Report_thread {

  var count_df:Long = 0
  var failed_constraints_cnt = 0
  var succeeded_constraints_cnt = 0
  var tot_row_count = 0


  def getQC_report(args: Array[String], checks: Check, spark: SparkSession, sc: SparkContext, log: Logger, argspair: Map[String,String], HiveTableName: String):String = {
    // Check data quality measures -- this is only for generic DQ report
    import spark.implicits._
    println("Inside QC_report")
    val qc = new QualityCheckerGlobal
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

    val df = DataQualityFactoryPattern.determineFileFormat(formatType, spark, sc, ip_file, ipSchemaPath, HiveTableName, val_sep, val_header, val_infr_schema, val_null_vl, val_charset, multi_line_flg, Dataset_Multi_Line_quote, Dataset_Multi_Line_escape)

    /*
         val rawData = Seq(
          (1, "AB", "awesome thing.","high", 0),
          (2, "AD", "available at xyz", "high", 1),
          (3, "AE",null, "low", 5),
          (4, "A", "checkout abc", "high", 10),
          (5, "D", "abc", "high", 12))

        val cols = Seq("product_id", "order_id", "description", "priority", "numViews")
        val df = spark.createDataFrame(rawData).toDF(cols: _*)*/

    val df_col_list = df.columns.toArray //will be used in UDF

    count_df = df.count()

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

    val qualityResult = qc.do_check(df, checks)
    log.info("log: qualityResult------" + qualityResult)

    val parsedQualityResultDF = QualityResultParser.parse_results(qualityResult, spark, count_df)

    def md5HashString(s: String): String = {
      import java.security.MessageDigest
      import java.math.BigInteger
      val md = MessageDigest.getInstance("MD5")
      val digest = md.digest(s.getBytes)
      val bigInt = new BigInteger(1,digest)
      val hashedString = bigInt.toString(16)
      //   val hashedString = s.hashCode.toString
      hashedString
    }

    val md5Hash = spark.udf.register("md5Hash",md5HashString _)


    def extractColumnNames(validation_rule: String): String = {
      System.out.println("Inside extract column name routine ")
      var extractedColumnList = ""
      //pattern to extract words with alpabets, number and underscore
      val extrctPssblColNmesRegex = "[a-zA-Z0-9_]*".r
      //from validation rule, extract words based on provided regex and convert them into array
      val extractedPossiblColNames = extrctPssblColNmesRegex.findAllIn(validation_rule).toArray
      //compare column list from user provided sql (via cols stored in dataframe) and compare the column names extracted from
      //validation rule and identify the matching col names.
      extractedColumnList = df_col_list.intersect(extractedPossiblColNames).mkString(", ")

      if(extractedColumnList == ""){
        extractedColumnList="NA"
      }
      System.out.println("Validation Rule: " + validation_rule + " SQL Column List: " + df_col_list + " Extracted values: " + extractedColumnList)
      extractedColumnList
    }

    val extrctColNames = spark.udf.register("extrctColNames",extractColumnNames _)
    parsedQualityResultDF.createOrReplaceTempView("dqCheckStagingTbl")
    //val dqCheckStagingTblOp = spark.sql("select * from dqCheckStagingTbl")
    var dqCheckStagingTblOp = spark.sql("select Constraint_rule,Validation_rule,constraint_msg,resulting_status,Total_rows,Pass_count,Pass_percentage,Fail_count,Fail_percentage,uniq_rule_identifier,rule_id, Name from dqCheckStagingTbl")
      .withColumn("uniq_rule_identifier",when(trim(col("uniq_rule_identifier")) === lit("uniq_rule_identifier:"),(col("validation_rule")))
        .otherwise(regexp_replace(col("uniq_rule_identifier"),"uniq_rule_identifier:","")))
      .withColumn("Trend_key",concat(lit(appName),lit("_"),col("validation_rule")))
      .withColumn("Trend_key_hash",md5Hash(concat(lit(appName),lit("_"),col("validation_rule"))))
      .withColumn("rule_name", trim(regexp_replace(substring_index(col("validation_rule"), "{", 1),lit("name: "),lit(""))))
      //In the above calculation, we are trying to fetch rule from column validation_rule. Ex: 'name: isComplete{column: fsc_acct_fimp}' --> isCompleteq
      .withColumn("input_data", lit(input_data))
      .withColumn("Hive_inputTable", lit(table_string))
      .withColumn("insert_timestamp", lit(from_unixtime(unix_timestamp(lit(current_Timestamp), "yyyy-MM-dd_hhmmss"))))
      .withColumn("job_run_id",lit(appId))
      .withColumn("Dataset_name",lit(dataset_name))
      .withColumn("column_list",extrctColNames(col("validation_rule")))
      //.withColumn("rule_name",extrctRuleName(col("validation_rule")))
      .withColumn("currentDate", lit(from_unixtime(unix_timestamp(lit(current_Date), "yyyy-MM-dd"))))


    val ConstraintRegex = dqCheckStagingTblOp.withColumn("constraint_1", regexp_replace(dqCheckStagingTblOp("Constraint_rule"), "\\(,", "(")).drop("Constraint_rule")
    val ConstraintRegex_1 = ConstraintRegex.withColumn("Constraint_rule_1", regexp_replace(ConstraintRegex("constraint_1"), "\\(List", "")).drop("constraint_1")
    val dqCheckStg = ConstraintRegex_1.withColumn("Constraint_rule", regexp_replace(ConstraintRegex_1("Constraint_rule_1"), "\\(LENGTH", "")).drop("Constraint_rule_1")
      .withColumn("hive_dataset", lit(HiveTableName))
      .withColumn("column_name", getColumnNameUdf(col("Constraint_rule")))
      .withColumn("temp_rule_name", getRuleNameUdf(col("Constraint_rule")))
      .withColumn("rule_name", getFinalRule(col("temp_rule_name"), col("Name"))).drop("temp_rule_name")
      .withColumn("dq_rules", getDQRuleUdf(col("Constraint_rule"), col("validation_rule")))
      .withColumn("dq_rule_description", getFinalDQRuleDescription(col("validation_rule"), col("dq_rules"),col("rule_name"))).drop("dq_rules")
      .withColumn("constraint_messages", correctConsMsg(col("constraint_msg")))
      .withColumn("Value",regexp_extract($"constraint_msg","Value: (-?\\ *[0-9]+\\.?[0-9]*(?:[Ee]\\ *-?\\ *[0-9]+)?)",1))
      .withColumn("Value_numeric",col("Value").cast("Decimal(22,10)"))
      .withColumn("get_pass_count", getPassCount(col("resulting_status"),col("total_rows"),col("pass_count")))
      .withColumn("get_fail_count", getFailCount(col("resulting_status"),col("fail_count")))

    dqCheckStg.createOrReplaceTempView("tempTable")
    //.withColumn("appName", lit(appName))
    //withColumn("Trend_key_hash",lit((concat(lit(appName),lit("_"),col("validation_rule"))).hashCode().toString()))
    print("---------------------------Target Schema-----------------------")
    dqCheckStagingTblOp.printSchema()
    //    dqCheckStagingTblOp.show(10,false)
    //  print(dqCheckStagingTblOp.printSchema())

    failed_constraints_cnt = dqCheckStagingTblOp.filter(dqCheckStagingTblOp("resulting_status") === "Failure").count().toInt
    succeeded_constraints_cnt = dqCheckStagingTblOp.filter(dqCheckStagingTblOp("resulting_status") === "Success").count().toInt

    return("qc_port_completed_successfully")
  }


  def getmetrics_report(args: Array[String], metrics_rules: List[Map[String, Any]], spark: SparkSession, sc: SparkContext, log: Logger,argspair: Map[String,String]) = {

    // Check data quality measures --- profiling & bucketting
    println("Inside QC_report")
    val qc = new QualityCheckerGlobal
    val datetimeNow = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))
    val kvpairs = args.grouped(2).collect { case Array(k, v) => k -> v }.toMap
    log.info("kvparis for file processing-----" + kvpairs)
    println(kvpairs)
    val formatType = kvpairs("format")
    val dq_IpHiveQuery = kvpairs("dq_IpHiveQuery")
    // val opFilePath = kvpairs("opFilePath")
    val appName = kvpairs("appName")
    val ipSchemaPath = kvpairs("ipSchemaPath")
    println("qualityResult")
    val val_sep = kvpairs("val_sep")
    val appId = kvpairs("appId")
    val val_header = kvpairs("val_header")
    var val_infr_schema = kvpairs("val_infr_schema")
    val val_null_vl = kvpairs("val_null_vl")
    val val_charset = kvpairs("val_charset")
    val Output_profiling_database = kvpairs("Output_profiling_database")
    val Output_profiling_table = kvpairs("Output_profiling_table")
    val multi_line_flg = kvpairs("multi_line_flg")
    val Dataset_Multi_Line_quote = kvpairs("Dataset_Multi_Line_quote")
    val Dataset_Multi_Line_escape = kvpairs("Dataset_Multi_Line_escape")
    //val historical_query = kvpairs("historical_query")
    //val ip_file = ""
    val ip_file = kvpairs("input_file")
    val outputFilename = ""
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

    //val outputFilename = "%s/%s/%s_stats".format(dqChkConsOpPath, datetimeNow, outputBaseFilename)
    val df = DataQualityFactoryPattern.determineFileFormat(formatType, spark, sc, ip_file, ipSchemaPath, consolidatedSql, val_sep, val_header, val_infr_schema, val_null_vl, val_charset, multi_line_flg, Dataset_Multi_Line_quote, Dataset_Multi_Line_escape)
    //df.cache()
    val count_df = df.count()
    log.info("Rowcount of Spark dataframe-----" + count_df)
    println("Rowcount of Spark dataframe-----" + count_df)

    val metricsCheckers = BuildProfiling.build(metrics_rules,df,log)
    val analysisResult: AnalyzerContext = { metricsCheckers.run()}

    println("analysisResult is: " + analysisResult)

    def createDate(year: Int, month: Int, day: Int): Long = {
      LocalDate.of(year, month, day).atTime(0, 0, 0).toEpochSecond(ZoneOffset.UTC)
    }

    val name_list = List("Compliance")
    import spark.implicits._
    val metricsdf1 = successMetricsAsDataFrame(spark, analysisResult)
    metricsdf1.show(10)
    //log.info(metricsdf1.show(30))
    val metricsdf = metricsdf1.filter(col("name") =!= "Histogram.bins")
      .withColumn("Metric_Ratio_Value",when(col("name").isin(name_list:_*),metricsdf1("value")*100).otherwise(null))
      .withColumn("value",when(col("name").isin(name_list:_*),metricsdf1("value")*count_df).otherwise(metricsdf1("value")))
      .withColumn("_instance", split(col("instance"), "_(?!.*_)"))
      .withColumn("instance", when(col("name") === "Compliance", $"_instance"(0)).otherwise(col("instance")))
      .withColumn("name", when(col("name") === "Compliance", $"_instance"(1)).otherwise(col("name")))
      .drop("_instance")
      .withColumnRenamed("value", "Metric_Abs_Value")
      .withColumnRenamed("name", "Metric_name")
      .withColumnRenamed("instance", "Column_Value")

    /*
    Commented as we no more utilize deequ's histogram. custom script has been written to handle histogram.
    val df_clean_histogram = metricsdf.filter(col("Metric_name").rlike("Histogram")).select('entity,'Column_Value,regexp_replace('Metric_name, "[.](?:ratio|abs)[.]", ".") as 'Metric_name,
      coalesce('Metric_Ratio_Value, 'Metric_Abs_Value) as 'value,expr("IF(instr(Metric_name,'.ratio.') > 0, 'Metric_Ratio_Value','Metric_Abs_Value') as type")
    ).groupBy('entity, 'Column_Value, 'Metric_name)
      .pivot("type", Seq("Metric_Abs_Value","Metric_Ratio_Value"))
      .agg(first('value)) */
    //metricsdf = metricsdf.withColumn("value",when(col("name").isin(name_list:_*),metricsdf("value")*count_df))

    log.info(metricsdf.show())
    //val repository = new FileSystemMetricsRepository(spark,dqChkConsOpPath)
    //repository.save(ResultKey(dateTime, Map("tag" -> "repositoryfile")))

    val Dataset_Name =  if (formatType == "hive") {
      val logicalPlan = spark.sessionState.sqlParser.parsePlan(consolidatedSql)
      import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
      val tables =  logicalPlan.collect { case r: UnresolvedRelation => r.tableName }
      tables.mkString(",")
    } else { "NA"}
    log.info("log: Dataset_Name------" + Dataset_Name)

    // val dbkey: Map[Int, Int] = dbValuePairs.map(x => x).zipWithIndex.map(t => (t._2, t._1)).toMap

    val argspair_key = argspair.-("ip_file").-("hiveDB").-("user_config").-("env").-("exec_mode").mapValues(_.toString)
    val dbkeyColumnSeq = argspair_key.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq

    //val metricsdf_combined = metricsdf.filter(!col("Metric_name").rlike("Histogram")).union(df_clean_histogram)
    //log.info(metricsdf_combined.show(20))
    //metricsdf_combined.createOrReplaceTempView("dqMetricsStagingTbl")

    metricsdf.createOrReplaceTempView("dqMetricsStagingTbl")

    val current_Timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd_HHmmss"))
    val current_Date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
    val year_month = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMM"))
    val dqMetrics = spark.sql("select * from dqMetricsStagingTbl")
      .withColumn("Dataset_Name", lit(Dataset_Name))
      .withColumn("insert_timestamp", lit(current_Timestamp))
      .withColumn("appId",lit(appId))
      .withColumn("currentDate", lit(current_Date))
      .withColumn("indicator", map(dbkeyColumnSeq:_*))
      .withColumn("appName", lit(appName))
      .withColumn("year_month",lit(year_month))
      .withColumnRenamed("column_value", "column_name")

    DataCheckImplementation.mail_config += ("Identifier" -> argspair_key.map{case(k,v) => k + ":" + v}.mkString("\n"))
    //val dqhawk_freq = spark.sql("select max(value) from ")
    dqMetrics.createOrReplaceTempView("tempTable")
    spark.sql("create table if not exists " + Output_profiling_database + "." + Output_profiling_table +  " AS select * from tempTable")

    val partition_cols = List("appname","year_month")

    DataCheckImplementation.hiveSave_profiling(dqMetrics: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, Output_profiling_database: String, Output_profiling_table: String)

    //------------------

    //All the profiling/bucketing rules gets triggered here and the final hive insert also happens from here

    val df_category_frequency = transformation_util.build_category(metrics_rules,df,spark,sc)
    df_category_frequency.createOrReplaceTempView("dqCategoryStagingTbl")
    val dqMetrics_category = spark.sql("select * from dqCategoryStagingTbl")
      .withColumn("entity",lit("column"))
      .withColumn("Dataset_Name", lit(Dataset_Name))
      .withColumn("insert_timestamp", lit(current_Timestamp))
      .withColumn("appId",lit(appId))
      .withColumn("currentDate", lit(current_Date))
      .withColumn("indicator", map(dbkeyColumnSeq:_*))
      .withColumn("appName", lit(appName))
      .withColumn("year_month",lit(year_month))

    DataCheckImplementation.mail_config += ("Identifier" -> argspair_key.map{case(k,v) => k + ":" + v}.mkString("\n"))

    val columns: Array[String] = dqMetrics_category.columns
    val reorderedColumnNames: Array[String] = Array("entity","column_name","metric_name","Metric_Abs_Value","Metric_Ratio_Value","Dataset_Name","insert_timestamp","appId","currentDate","indicator","appName","year_month")
    val df_final_freq_final: DataFrame = dqMetrics_category.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    //df_final_freq_final.write.mode("append").insertInto(Output_profiling_database + "." + Output_profiling_table)
    DataCheckImplementation.hiveSave_profiling(df_final_freq_final: DataFrame,partition_cols, spark: SparkSession, sc: SparkContext, Output_profiling_database: String, Output_profiling_table: String)
    //df_final_freq_final.show(1, false)

    //-------------------
    //val df_freq = spark.sql("SELECT instance,name, max(value) FROM "+ hiveDB + "." + Outputhive_table + "WHERE name RLIKE 'Histogram.ratio' group by instance")
    // Find failing row
  }

  def processHistoricaldata(args: Array[String], anomaly_rules: List[Map[String, Any]], spark: SparkSession, sc: SparkContext, log: Logger, argspair: Map[String,String]) = {

    //spark.sql(consolidatedSql).show()
    val kvpairs = args.grouped(2).collect { case Array(k, v) => k -> v }.toMap
    log.info("kvparis for file processing-----" + kvpairs)
    println(kvpairs)

    //val dbkey: Map[Int, Int] = dbValuePairs.map(x => x).zipWithIndex.map(t => (t._2, t._1)).toMap
    //val dbkeyColumnSeq = dbkey.flatMap(t => Seq(lit(t._1), lit(t._2))).toSeq
    BuildAnomalyChecker.build(anomaly_rules,argspair,kvpairs,spark,sc,log)
  }
}


/**
 * FindFaillingRows.
 * @global
 */


class FindFaillingRowsGlobal{

  val failingString = udf((value: String,
                           columnName : String,
                           isNullable: Boolean,
                           matches: String,
                           minLength: Int,
                           maxLength: Int) => {

    val matchRegex = matches match {
      case "" => {
        new Regex("(.*?)")
      }
      case s:String => {
        new Regex(s)
      }
    }

    var reason = ""
    var errorCount = 0

    if ((isNullable == false) && (value == null)) {
      errorCount = errorCount + 1
      reason = reason + " %s - is null".format(errorCount)
      reason
    }

    if (errorCount == 0){
      reason = value match {
        case matchRegex(_*) => {
          reason
        }
        case _ => {
          errorCount = errorCount + 1
          reason = reason + " %s -  does not match the following regular expression: %s".format(errorCount, matches)
          reason
        }
      }

      if (value != null){
        if (value.length < minLength) {
          errorCount = errorCount + 1
          reason = reason + " %s - has less than %s charaters".format(errorCount, minLength)
        }
      }

      if (value != null) {
        if (value.length > maxLength) {
          errorCount = errorCount + 1
          reason = reason + " %s - has more than %s charaters".format(errorCount, maxLength)
        }
      }
    }

    reason

  })


  val failingInt = udf((value: String, columnName : String, isNullable: Boolean, minValue: Int, maxValue: Int) => {
    var reason = ""
    var errorCount = 0

    if ((isNullable == false) && (value == null)) {
      errorCount = errorCount + 1
      reason = reason + " %s - is null".format(errorCount)
      reason
    }

    if (errorCount == 0){
      if (value != null){
        if (value.length < minValue) {
          errorCount = errorCount + 1
          reason = reason + " %s - is less than %s ".format(errorCount, minValue)
        }
      }


      if (value != null){
        if (value.length > maxValue) {
          errorCount = errorCount + 1
          reason = reason + " %s - is greater than %s ".format(errorCount, maxValue)
        }
      }
    }

    reason
  })



  def find(data_df :org.apache.spark.sql.DataFrame, spark: SparkSession, sc: SparkContext, constraints: List[RowChecker]): DataFrame = {

    var invalidRowsDf = spark.createDataFrame(sc.emptyRDD[Row],data_df.schema)
    invalidRowsDf = invalidRowsDf.withColumn("failing_column",lit(""))
    invalidRowsDf = invalidRowsDf.withColumn("failing_reason",lit(""))

    for (constraint <- constraints){
      constraint match {
        case c: RowCheckerString =>{
          val schema = RowLevelSchema().withStringColumn(name=c.columnName,
            isNullable=c.isNullable,
            minLength=c.minLength,
            maxLength=c.maxLength,
            matches=c.matches)

          val result = RowLevelSchemaValidator.validate(data_df, schema)
          var invalidRowsPerConstraint = result.invalidRows
          invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_column",lit(c.columnName))
          invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_reason", failingString(
            col(c.columnName),
            lit(c.columnName),
            lit(c.isNullable),
            lit(c.matches.getOrElse("")),
            lit(c.minLength.getOrElse(Int.MinValue)),
            lit(c.maxLength.getOrElse(Int.MaxValue))
          ))

          invalidRowsDf = invalidRowsDf.union(invalidRowsPerConstraint)
     }

        case c: RowCheckerInt =>{
          val schema = RowLevelSchema().withIntColumn(name=c.columnName, isNullable=c.isNullable, minValue=c.minValue, maxValue=c.maxValue)
          val result = RowLevelSchemaValidator.validate(data_df, schema)
          var invalidRowsPerConstraint = result.invalidRows
          invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_column",lit(c.columnName))
          invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_reason", failingInt(
            col(c.columnName),
            lit(c.columnName),
            lit(c.isNullable),
            lit(c.minValue.getOrElse(Int.MinValue)),
            lit(c.maxValue.getOrElse(Int.MaxValue))
          ))
          invalidRowsDf = invalidRowsDf.union(invalidRowsPerConstraint)
        }
      }
    }
    return invalidRowsDf
  }
}


