package com.dq.check.service

import java.io.{File, FileInputStream}
import java.nio.file.Paths
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.math.BigDecimal
import java.math.RoundingMode

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel}
//import org.apache.spark.ml.Pipeline
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.immutable.Map
import scala.io.Source
import scala.reflect.runtime.{universe => ru}
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.util.parsing.json._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.amazon.deequ.analyzers.runners.{AnalysisRunBuilder, AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import org.apache.spark.sql.types.DecimalType

case class ParsedResult(column_name: String, constrain: String, status: String, constrain_type: String, coverage: Double)

abstract class RowChecker(columnName: String,
                          isNullable: Boolean)

case class RowCheckerString(columnName: String,
                            isNullable: Boolean,
                            matches: Option[String]=None,
                            minLength: Option[Int]=None,
                            maxLength: Option[Int]=None) extends RowChecker(columnName,isNullable)

case class RowCheckerInt(columnName: String,
                         isNullable: Boolean,
                         minValue: Option[Int]=None,
                         maxValue: Option[Int]=None) extends RowChecker(columnName,isNullable)


abstract class AbstractQualityChecker {
  def do_check(data_df: DataFrame) : VerificationResult
}

abstract class AbstractFindFaillingRows {
  def find(data_df: DataFrame, spark: SparkSession, sc: SparkContext) : DataFrame
  val failingString = udf((value: String,
                           columnName : String,
                           isNullable: Boolean,
                           matches: String,
                           minLength: Int,
                           maxLength: Int) => {


    var reason = ""
    var errorCount = 0

    val matchRegex = matches match {
      case "" => new Regex("(.*?)")
      case s:String => new Regex(matches)
    }

    if ((isNullable == false) && (value == null)) {
      errorCount = errorCount + 1
      reason = reason + " %s - is null".format(errorCount)
    }

    if (errorCount == 0){
      reason = value match {
        case matchRegex(_*) => {
          reason
        }
        case _ => {
          errorCount = errorCount + 1
          reason = reason + " %s does not match the following regular expression: %s".format(errorCount, matches)
          reason
        }
      }

      if (value != null){
        if (value.length < minLength) {
          errorCount = errorCount + 1
          reason = reason + " %s has less than %s charaters".format(errorCount, minLength)
        }
      }

      if (value != null){
        if (value.length > maxLength) {
          errorCount = errorCount + 1
          reason = reason + " %s has more than %s charaters".format(errorCount, maxLength)
        }
      }
    }
    reason
  })

  val failingInt = udf((value: String,
                        columnName : String,
                        isNullable: Boolean,
                        minValue: Int,
                        maxValue: Int) => {


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
          reason = reason + " %s is less than %s ".format(errorCount, minValue)
        }
      }

      if (value != null){
        if (value.length > maxValue) {
          errorCount = errorCount + 1
          reason = reason + " %s is greater than %s ".format(errorCount, maxValue)
        }
      }
    }
    reason
  })
}

object SaveReport {
  def savefile(data_df: DataFrame, hdfs_path:String,report_details: List[String], sheet_name: String, savemode:String = "append"):String = {

    var filePath = ""
    val col_length = data_df.columns.size
    val count = data_df.count() + 1
    val char_result = (64 + col_length).toChar + count.toString
    println("report_type: " + report_details(0))
    if(report_details(0)=="csv"){
      val filenamewithoutextension = report_details(1).split("\\.")(0)
      val filePath_csv = Paths.get(hdfs_path, filenamewithoutextension).toString
      data_df.coalesce(1).write.mode("overwrite").format("csv").option("emptyValue", null).option("nullValue", null).option("header", "true").save(filePath)
      filePath = filePath_csv.concat(report_details(1))
    }
    if(report_details(0)=="excel" || report_details(0)=="xlsx"){
      //val filenamewithoutextension = report_details(1).split("\\.")(0)
      filePath = Paths.get(hdfs_path, report_details(1)).toString
      println("excel filepath: " + filePath)
      data_df.coalesce(1).write.format("com.crealytics.spark.excel").option("dataAddress", sheet_name + "!A1:" + char_result).option("header","true").mode(savemode).save(filePath)
    }
    return filePath
  }
}

object ConfigParser {
  def parse(configFile: String, save_bad_records: String): String = {
    if (save_bad_records.toBoolean) {
      val constraint_filename = configFile.split("/").last
      val json_content = Source.fromFile(constraint_filename).getLines.mkString
      println(json_content)
      //val json_content = scala.io.Source.fromFile(configFile).mkString
      val json_data = JSON.parseFull(json_content)
      //json_data.getOrElse(null).asInstanceOf[Map[String, Any]]
      return json_content
      //val json = JSON.parseFull(Source.fromInputStream(getClass().getResourceAsStream(configFile)).mkString)
    }
    else
    {
      val configFile = "/rowlevel_check.json"
      //val jsonString = "{\"entity_name\":\"value1\",\"rules\":{\"CHD_ACCOUNT_NUMBER\":[{\"name\":\"hasPattern\",\"args\": {\"pattern\": \"[0-9]{8}\" }}]     }}"
      val json_content = Source.fromInputStream(getClass().getResourceAsStream(configFile)).mkString
      //val json_data = JSON.parseFull(Source.fromInputStream(getClass().getResourceAsStream(configFile)).mkString)
      //return json_data.getOrElse(null).asInstanceOf[Map[String, Any]]
      return json_content

    }
    //selectedCheckResults.flatMap { constraintResult =>
    //              constraintResult.constraint.toString,
    //              constraintResult.status.toString,
    //             constraintResult.message.getOrElse("")}
  }
  //Define Logger properties
  def log4jSetConfig(jobApplicationName: String, appID: String, logPropFilePath: String, basePath: File): Unit = {
    val applicationId = appID + ".log"
    val prop = new Properties()
    val file = new FileInputStream(logPropFilePath)
    try {
      prop.load(file)
    }
    finally {
      file.close()
    }
    val fullLogFilePath = basePath +"/"+ jobApplicationName
    val path = new File(fullLogFilePath)
    println("path" + path)
    try {
      if (!path.exists()) {
        path.mkdirs()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(2)
    }
    //System.setProperty("logfile.name", path + "/" + applicationId)
    //println("The absolute log path location is -----> "+System.setProperty("logfile.name", path + "/" + applicationId))
    //PropertyConfigurator.configure(prop)
  }
}

/*
object PerformHash{

  import org.apache.spark.sql.functions.{col, udf}
  import java.security.MessageDigest
  import java.math.BigInteger

  def md5HashString(s: String): String = {
    import java.security.MessageDigest
    import java.math.BigInteger
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }

  val md5Hash = spark.udf.register("md5Hash",md5HashString(_))

}*/
/**
 * @var		object	QualityResultParser
 * @global
 */
object QualityResultParser{
  /**
   * @param	quality_check_results	:VerificationResult
   * @param	spark                	:SparkSession
   * @return	mixed
   */
  def parse_results(quality_check_results :VerificationResult, spark :SparkSession, count_df: Float): DataFrame = {
    import spark.implicits._

    val selectedCheckResults = quality_check_results.checkResults.values.toSeq
    val resultDataFrame = VerificationResult.checkResultsAsDataFrame(spark, quality_check_results)

    println("--------Initial Result---------------")
    println(resultDataFrame.show(truncate=false))
    var df_filter = resultDataFrame.drop("check", "check_level", "check_status")

    df_filter = df_filter
      .withColumn("constraint_msg", split(col("constraint_message"), "&")(0))
      .withColumn("UniqueID", split(col("constraint_message"), "&")(1))
      .withColumn("Failure_Flag", split(col("constraint_message"), "&")(2))
      .withColumn("uniq_rule_identifier",when(split(col("constraint_message"), "&")(3) === "",
        col("UniqueID"))
        .otherwise(split(col("constraint_message"), "&")(3)))

    println("------------Initial processed df----------------")
    df_filter.show(truncate=false)
    df_filter = df_filter.drop("constraint_message")
    df_filter.createTempView("dataQualityResultDF")

    val df_tmp = df_filter.withColumn("constraint_msg",when(col("constraint_status") === "Success",
      regexp_replace(df_filter("constraint_msg"), "should be", "is"))
      .otherwise(col("constraint_msg")))

    var df = df_tmp.groupBy("constraint","UniqueID")
      .agg(collect_set($"constraint_status") as "combined_status",
        collect_set($"constraint_msg") as "constraint_msg",
        collect_set($"uniq_rule_identifier") as "uniq_rule_identifier")
      .withColumn("resulting_status",when(array_contains($"combined_status",
        lit("Failure")),"Failure")
        .otherwise("Success"))

    println("---schema for df----")
    df.printSchema()
    df.show()

    val name_list = List("isUnique","isPrimaryKey","hasUniqueness","hasDistinctness","isComplete","hasUniqueValueRatio","hasMutualInformation","hasPattern","containsCreditCardNumber","containsEmail","containsURL","containsSocialSecurityNumber","hasDataType","hasCompleteness",
      "satisfies","isNonNegative","isPositive","isLessThan","isLessThanOrEqualTo","isGreaterThan","isGreaterThanOrEqualTo","isContainedIn")
    val myExpression = "(1-Value_numeric)*Total_rows"
    val failPercentExpr = "(1-Value_numeric)*100"

    df = df
      .withColumn("constraint_msg", concat_ws("&", $"constraint_msg"))
      .withColumn("constraint", regexp_replace(df("constraint"), ",None", ""))
      .withColumn("UniqueID",split(df("UniqueID"), "\\|\\w+$").getItem(0))
      .withColumn("Value",regexp_extract($"constraint_msg","Value: (-?\\ *[0-9]+\\.?[0-9]*(?:[Ee]\\ *-?\\ *[0-9]+)?)",1))
      .withColumn("Value_numeric",col("Value").cast("Decimal(22,10)"))
      .withColumn("constraint_msg", regexp_replace($"constraint_msg", "&Value: [+-]?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?$", ""))
      .withColumn("Name",regexp_extract($"UniqueID","name: ([a-zA-Z]+)",1))
      .withColumn("Total_rows", lit(count_df))
      .withColumn("Name", regexp_replace($"Name", " ", ""))
      .withColumn("Pass_count",when(col("Name").isin(name_list:_*),round(col("Value_numeric") * count_df,0)).otherwise("NA"))
      .withColumn("Pass_percentage",when(col("Name").isin(name_list:_*),round($"Value_numeric" * 100,2)).otherwise(0.0))
      .withColumn("Fail_count",when(col("Name").isin(name_list:_*),round(expr(myExpression),0)).otherwise("NA"))
      .withColumn("Fail_percentage",when(col("Name").isin(name_list:_*),round(expr(failPercentExpr),2)).otherwise(0.0))
      .withColumn("uniq_rule_identifier",$"uniq_rule_identifier".getItem(0))
      .withColumn("rule_id",lit("")) //place holder column, will be updated in upcoming release
      .withColumnRenamed("UniqueID", "Validation_rule")
      .withColumnRenamed("constraint", "Constraint_rule")
    println("printschema" + df.printSchema())
    println(df.show(30,false))

    val df_updated = df.drop("Name")
      .drop("Value")
      .drop("Value_numeric")
      .drop("combined_status")
    //Constraint_rule,Validation_rule,constraint_msg,resulting_status,Total_rows,Pass_count,Pass_percentage,Fail_count,Fail_percentage,uniq_rule_identifier,rule_id
    return df_updated
  }
}

/**
 * @var		object	BuildChecks
 * @global
 */
object BuildChecks {
  /**
   * @param
   * @return mixed
   */

  //@scala.AnyRef
  //val list = rules.asInstanceOf[Map[String, List[Map[String, Any]]]]("rules")

  def build(rules: List[Map[String, Any]],log: Logger): Check = {
    var checks = Check(CheckLevel.Warning, "Data unit test")
    //val list = rules.asInstanceOf[Map[String, List[Map[String, Any]]]]("rules")

    var name = "".asInstanceOf[Any]
    var Failure_Flag = "".asInstanceOf[Any]
    var args = "".asInstanceOf[Any]
    var obj = "".asInstanceOf[Any]
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val dq_rule = new builddeequ_rules

    try{

      for (qc_constraint <- rules) {
        log.info("qc_constraint" +  qc_constraint)
        name = qc_constraint("name")
        Failure_Flag = qc_constraint.getOrElse("Failure_Flag", "Info").toString()
        args = qc_constraint("args").asInstanceOf[Any]
        obj = dq_rule
        //Method itself as an object getOrElse("condition", "== 1")
        //val mtd = hiObj.getClass.getMethod(name.toString,getClassOf(args).runtimeClass)
        //@args Any
        //mtd.invoke(hiObj,args)

        println("--------Step-1-------------------------------")
        val instanceMirror: ru.InstanceMirror = mirror.reflect(obj)
        println("--------Step-2-------------------------------")
        // ------ creates an instance of builddeequ_rules ------
        val classSymbol = mirror.classSymbol(obj.getClass)

        // ------ sets the method which is available in "name" val ------
        val termName: ru.MethodSymbol = classSymbol.info.decl(ru.TermName(name.toString)).asMethod
        val methodMirror: ru.MethodMirror = instanceMirror.reflectMethod(termName)

        // ------ Call the method name which was identified above and pass the argument using "args" variable ------
        println("------ Call the method name which was identified above and pass the argument using \"args\" variable ------")
        methodMirror.apply(args, Failure_Flag) }
    }
    catch {
      case e: Exception =>
        log.info("Failure happened!")
        log.info(e.getMessage)
        System.err.println("Failure happened!")
        System.err.println(e.getMessage)
        log.info(e.printStackTrace())
        System.exit(1)
    }

    val finalcheck = dq_rule.checks
    log.info("finalcheck-------->" + finalcheck)
    println("finalcheck-------->" + finalcheck)

    //whatever build above gets passed to below variable
    return finalcheck
  }
}


object BuildPrepocessing {
  /**
   * @param
   * @return mixed
   */
  //@scala.AnyRef
  /*  def build(rules: List[Map[String, Any]],log: Logger): Pipeline = {
      //val list = rules.asInstanceOf[Map[String, List[Map[String, Any]]]]("rules")
      try{
        for (qc_constraint <- rules) {
          log.info("qc_constraint" +  qc_constraint)
          val name = qc_constraint("name")
          val args = qc_constraint("args")
          val obj = buildpreprocessing_rules
          // Method itself as an object

          //val mtd = hiObj.getClass.getMethod(name.toString,getClassOf(args).runtimeClass)
          //@args Any
          //mtd.invoke(hiObj,args)
          val mirror = ru.runtimeMirror(getClass.getClassLoader)
          val instanceMirror: ru.InstanceMirror = mirror.reflect(obj)
          val classSymbol = mirror.classSymbol(obj.getClass)
          val termName: ru.MethodSymbol = classSymbol.info.decl(ru.TermName(name.toString)).asMethod
          val methodMirror: ru.MethodMirror = instanceMirror.reflectMethod(termName)
          methodMirror.apply(args) }
      }
      catch {
        case NonFatal(e) =>
          log.info("Failure happened!")
          log.info(e.getMessage)
          System.err.println("Failure happened!")
          System.err.println(e.getMessage)
          log.info(e.printStackTrace())
          System.exit(1)
      }
      def finalpipeline = com.citi.dq.buildpreprocessing_rules.pipeline
      return finalpipeline
    }*/
}

object BuildProfiling {
  /**
   * @param
   * @return mixed
   */
  //@scala.AnyRef
  def build(rules: List[Map[String, Any]],dataFrame: DataFrame, log: Logger): AnalysisRunBuilder = {
    //val list = rules.asInstanceOf[Map[String, List[Map[String, Any]]]]("rules")
    var analysisResult = AnalysisRunner.onData(dataFrame)
    try{
      for (qc_constraint <- rules) {
        log.info("qc_constraint" + qc_constraint)
        println("qc_constraint" + qc_constraint)
        val name = qc_constraint("name")
        val args = qc_constraint("args")
        val arg = args.asInstanceOf[Map[String,Any]]
        val columnName = arg.getOrElse("column",List("test")).asInstanceOf[List[String]]
        name match {
          case "approxcountdistinct" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(ApproxCountDistinct(column))
            }
          case "mean" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Mean(column)) }
          case "frequency" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Histogram(column)) }

          case "distinctness" =>
            analysisResult = analysisResult.addAnalyzer(Distinctness(columnName))
          case "completeness" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Completeness(column)) }

          case "uniqueness" =>
            analysisResult = analysisResult.addAnalyzer(Uniqueness(columnName))

          case "maxlength" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(MaxLength(column))
            }
          case "minlength" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(MinLength(column)) }

          case "countdistinct" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(CountDistinct(column)) }

          case "uniquevalueratio" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(UniqueValueRatio(columnName))
            }
          case "approxquantile" =>
            for (column <- columnName) {
              val quantile = arg.getOrElse("quantile","").asInstanceOf[Double]
              analysisResult = analysisResult.addAnalyzer(ApproxQuantile(column,quantile)) }

          case "patternmatch" =>
            val regpattern = arg("Pattern").toString
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(PatternMatch(column,regpattern.r))     }

          case "minimum" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Minimum(column))  }

          case "maximum" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Maximum(column)) }

          case "entropy" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Entropy(column)) }

          case "standardDeviation" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(StandardDeviation(column)) }

          case "size" =>
            analysisResult = analysisResult.addAnalyzer(Size())

          case "compliance" =>
            for (column <- columnName) {
              val instance = column + "_" + arg.getOrElse("instance","test").asInstanceOf[String]
              val condition = arg.getOrElse("condition","").asInstanceOf[String]
              val predicate  = column + " " + condition
              analysisResult = analysisResult.addAnalyzer(Compliance(instance,predicate))
            }

          case "mutualinformation" =>
            analysisResult = analysisResult.addAnalyzer(MutualInformation(columnName))

          case "datatype" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(DataType(column)) }

          case "correlation" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Correlation(column,column)) }

          case "approxquantiles" =>
            for (column <- columnName) {
              val quantiles = arg.getOrElse("quantiles","").asInstanceOf[List[Double]]
              analysisResult = analysisResult.addAnalyzer (ApproxQuantiles(column, quantiles))
            }
          case "sum" =>
            for (column <- columnName) {
              analysisResult = analysisResult.addAnalyzer(Sum(column)) }
          case _ =>
            println("Default case")
        }
      }
    }
    catch {
      case e: Exception =>
        log.info("Failure happened!")
        log.info(e.getMessage)
        System.err.println("Failure happened!")
        System.err.println(e.getMessage)
        log.info(e.printStackTrace())
        System.exit(1)
    }
    log.info("analysisResult: " + analysisResult)
    println("analysisResult: " + analysisResult.toString)
    return analysisResult
  }
}

object BuildRowChecker{
  /**
   * @param	mixed
   * @return	mixed
   */
  def build(rules: Map[String, Any]): List[RowChecker] = {
    var constraints: List[RowChecker] = List()
    val columnList = rules.keys.toList
    println(rules)
    try {
      for (columnName <- columnList) {
        var matches: Option[String] = None
        var minValue: Option[Int] = None
        var maxValue: Option[Int] = None
        var isNullable = true
        val constraintslist = rules(columnName).asInstanceOf[List[Map[String, Any]]]
        println(constraintslist)
        println("constraintslist")
        for (qc_constraint <- constraintslist) {
          val constraintName = qc_constraint("name")
          constraintName match {
            case "isComplete" => {
              isNullable = false
            }
            case "hasPattern" => {
              val args = qc_constraint("args")
              val arg = args.asInstanceOf[Map[String, Any]]
              val pattern = arg("pattern").toString
              matches = Some(pattern)
            }
            case "isContainedIn" => {
              val args = qc_constraint("args")
              val arg = args.asInstanceOf[Map[String, Any]]
              val columnType = arg("type").toString
              columnType match {
                case "string" => {
                  val list = arg("list").toString
                  matches = Some(list)
                }
                case "double" => {
                  //val bounds = constraintMap.asInstanceOf[Map[String, Map[String, Double]]](constraintName)
                  //minValue = Some(Math.round(bounds.get("lowerBound").orElse(Some(Double.MinValue)).get.asInstanceOf[Double]).asInstanceOf[Int])
                  //maxValue = Some(Math.round(bounds.get("upperBound").orElse(Some(Double.MaxValue)).get.asInstanceOf[Double]).asInstanceOf[Int])
                }
              }
            }
            case _ => {
              println("Row Checker: No Case Statements Matched!!")
            }
          }
          val columnType = "string"
          val columnName = "string"
          columnType match {
            case "string" => {
              constraints = RowCheckerString(columnName = columnName, isNullable = isNullable, matches = matches, minLength = minValue, maxLength = maxValue) :: constraints
            }
            case "double" => {
              constraints = RowCheckerInt(columnName = columnName, isNullable = isNullable, minValue = minValue, maxValue = maxValue) :: constraints
            }
          }
        }
      }
    }
    catch {
      case e: Exception =>
        System.err.println("Failure happened!")
        System.err.println(e.getMessage)
        println(e.printStackTrace())
        System.exit(1)

    }
    println(constraints)
    return constraints
  }
}