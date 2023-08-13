package com.dq.check.service

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.col


object transformation_util {
  /**
   * @return mixed
   *         This scala file is for handling profiling and bucketting
   */

  val bucketedData_final_schema =
    StructType(
      StructField("column_name", StringType, true) ::
        StructField("Value", StringType, true) ::
        StructField("Metric_Abs_value", LongType, true) ::
        StructField("metric_name", StringType, true) ::
        StructField("Metric_Ratio_value", LongType, true) :: Nil)

  def subset_frame(beg_val:Int=0, end_val:Int)(df: DataFrame): DataFrame = {
    val sliceCols = df.columns.slice(beg_val, end_val)
    return df.select(sliceCols.map(name => col(name)):_*)
  }

  def bucketizeWithinPartition(df: DataFrame, splits: mutable.Map[String, Array[Double]], partitionCol: String, featureCol: String,spark: SparkSession): DataFrame = {

    import spark.implicits._

    val window = Window.partitionBy(partitionCol).orderBy($"bucket_start")

    val splitsDf = splits.toList.toDF(partitionCol, "splits")
      .withColumn("bucket_start", explode($"splits"))
      .withColumn("bucket_end", coalesce(lead($"bucket_start", 1).over(window), lit(Int.MaxValue)))
      .withColumn("bucket", row_number().over(window))
    val joinCond = "d.%s = s.%s AND d.%s >= s.bucket_start AND d.%s < bucket_end".format(partitionCol, partitionCol, featureCol, featureCol)
    df.as("d")
      .join(splitsDf.as("s"), expr(joinCond), "LEFT")
      .select($"d.*", $"s.bucket",$"s.bucket_start",$"s.bucket_end")
  }


  def build_category(rules: List[Map[String, Any]], dataFrame: DataFrame, spark: SparkSession, sc: SparkContext): DataFrame = {
    //val list = rules.asInstanceOf[Map[String, List[Map[String, Any]]]]("rules")
    var bucketedData_final = Seq[DataFrame]()
    val count_df = dataFrame.count()

    for (qc_constraint <- rules) {
      println("qc_constraint" + qc_constraint)
      val name = qc_constraint("name").toString.toLowerCase
      val args = qc_constraint("args")
      val arg = args.asInstanceOf[Map[String, Any]]
      val splits_map = mutable.Map.empty[String, Array[Double]]

      name match {
        case "histogram" => {
          val columnName = arg.getOrElse("column",List("test")).asInstanceOf[List[String]]
          println("column:   " + columnName)
          val dataFrame_selectcols = dataFrame.select(columnName.map(col): _*)
          val col_length = dataFrame_selectcols.columns.size
          var start = 0
          var end = col_length
          val batchSize = 20
          val count = col_length / batchSize
          val remainder = col_length % batchSize
          var i = 0
          var df_final = Seq[DataFrame]()
          while (i < count) {
            var subset_df: DataFrame = dataFrame_selectcols.transform(subset_frame(start, start + batchSize))
            subset_df = subset_df.columns.foldLeft(subset_df)((current, c) => current.withColumn(c, col(c).cast("string")))
            val cols = subset_df.columns
            var col_stack = new ListBuffer[String]()
            for (col <- cols) {
              col_stack += "'" + col + "'," + col
            }

            val df_melt = subset_df.selectExpr("stack(" + batchSize + ", " + col_stack.mkString(",") + ")")
              .withColumnRenamed("col0", "column_name")
              .withColumnRenamed("col1", "Value")
            ///val df_melt_null = df_melt.filter(col("Value").isNull || col("Value").isNaN).withColumnRenamed("Value","bucket_range").withColumn("bucket_",lit("NullValue"))
            //val window = Window.partitionBy(partitionCol)
            var bucketedData = df_melt.na.fill("Missing")
              .groupBy(col("column_name"),col("Value")).count()
              .withColumnRenamed("count","Metric_Abs_value")
            var bucketedData_range = bucketedData
              .withColumn("metric_name", concat(lit("Histogram_"), col("Value")))
              .withColumn("Metric_Ratio_value",bucketedData("Metric_Abs_value")/count_df)
            //bucketedData_range.show(10)
            //var bucketedData_range = bucketedData.withColumn("bucket_range", concat(lit("Bucket_"), col("bucket_start"), lit("-"), col("bucket_end")))
            df_final = df_final :+ bucketedData_range
            ///df_null_final = df_null_final :+ df_melt_null
            start = start + batchSize
            println("start" + start)
            i = i + 1
          }

          if (remainder != 0) {
            start = end - remainder
            var subset_df: DataFrame = dataFrame_selectcols.transform(subset_frame(start, end))
            subset_df = subset_df.columns.foldLeft(subset_df)((current, c) => current.withColumn(c, col(c).cast("string")))
            subset_df.show(10)

            val cols = subset_df.columns
            var col_stack = new ListBuffer[String]()
            for (col <- cols) {
              col_stack += "'" + col + "'," + col
            }
            val df_melt = subset_df.selectExpr("stack(" + remainder + ", " + col_stack.mkString(",") + ")").withColumnRenamed("col0", "column_name").withColumnRenamed("col1", "Value")
            ///val df_melt_null = df_melt.filter(col("Value").isNull || col("Value").isNaN).withColumnRenamed("Value","bucket_range").withColumn("bucket_",lit("NullValue"))
            //val window = Window.partitionBy(partitionCol)
            var bucketedData = df_melt.na.fill("Missing").groupBy(col("column_name"),col("Value")).count().withColumnRenamed("count","Metric_Abs_value")
            var bucketedData_range = bucketedData
              .withColumn("metric_name", concat(lit("Histogram_"), col("Value")))
              .withColumn("Metric_Ratio_value",bucketedData("Metric_Abs_value")/count_df)
            bucketedData_range.show(100)
            df_final = df_final :+ bucketedData_range
          }

          var bucketedData_transform = df_final.reduce(_ union _)
          bucketedData_final = bucketedData_final :+ bucketedData_transform
          ///var bucketedData_df_null = df_null_final.reduce(_ union _).groupBy(col("bucket"),col("column_name"),col("bucket_range")).count()
          ///bucketedData_df_null = bucketedData_df_null.withColumnRenamed("count","Metric_Abs_value")
        }
        case "category_frequency" => {
          for ((k,v) <- arg) {
            val category = arg(k).asInstanceOf[Map[String, Any]]
            val column = category("column").asInstanceOf[List[String]]
            val range = category("bucket").asInstanceOf[List[Double]].toArray
            println("column:   " + column)

            for (k1 <- column) {
              splits_map += (k1 -> (range))
            }
          }
          println("splits_map:   " + splits_map)
          //val bucketedData_df_null = dataFrame.filter(col("Value").contains("NullValue")).withColumnRenamed("Value","bucket_range").drop("appName","insert_timestamp","Dataset_Name","Identifier","appId","currentDate")
          val col_length = dataFrame.columns.size
          var start = 0
          var end = col_length
          val batchSize = 30
          val count = col_length / batchSize
          val remainder = col_length % batchSize
          var i = 0
          var df_final = Seq[DataFrame]()
          var df_null_final = Seq[DataFrame]()
          while (i < count) {
            var subset_df:DataFrame = dataFrame.transform(subset_frame(start, start + batchSize))
            subset_df = subset_df.columns.foldLeft(subset_df)((current, c) => current.withColumn(c, col(c).cast("double")))

            val cols = subset_df.columns
            var col_stack = new ListBuffer[String]()
            for (col <- cols){
              col_stack += "'"+ col + "'," + col
            }
            val df_melt = subset_df.selectExpr("stack(" + batchSize + ", " + col_stack.mkString(",") + ")").withColumnRenamed("col0","column_name").withColumnRenamed("col1","Value")
            ///val df_melt_null = df_melt.filter(col("Value").isNull || col("Value").isNaN).withColumnRenamed("Value","bucket_range").withColumn("bucket_",lit("NullValue"))
            val bucketedData = bucketizeWithinPartition(df_melt, splits_map, "column_name", "Value", spark)
            bucketedData.persist(StorageLevel.MEMORY_ONLY_2)
            var bucketedData_range = bucketedData.na.fill("Missing").withColumn("bucket_range",concat(lit("Bucket_"),col("bucket_start"),lit("-"),col("bucket_end")))

            bucketedData_range = bucketedData_range.withColumn("bucket_range", regexp_replace(bucketedData_range("bucket_range"), "-2.147483647E9|-Missing", ""))
            bucketedData_range = bucketedData_range.withColumn("bucket_range", regexp_replace(bucketedData_range("bucket_range"), "null", "Bucket_Missing"))
            bucketedData_range = bucketedData_range.withColumn("bucket_range",when(col("bucket_range").isNull || col("bucket_range").isNaN ,lit("Bucket_Missing")).otherwise(col("bucket_range")))
            //diffDf = diffDf.withColumn("Status",when(!col("psi") >= lowerbound || !col("psi") <= upperbound ,"SUCCESS"))
            df_final = df_final :+ bucketedData_range
            ///df_null_final = df_null_final :+ df_melt_null
            start = start + batchSize
            println("start" + start)
            i = i+1
          }

          if (remainder != 0) {
            start = end - remainder
            var subset_df:DataFrame = dataFrame.transform(subset_frame(start, end))
            subset_df = subset_df.columns.foldLeft(subset_df)((current, c) => current.withColumn(c, col(c).cast("double")))
            val cols = subset_df.columns
            var col_stack = new ListBuffer[String]()
            for (col <- cols){
              col_stack += "'"+ col + "'," + col
            }
            val df_melt = subset_df.selectExpr("stack(" + remainder + ", " + col_stack.mkString(",") + ")").withColumnRenamed("col0","column_name").withColumnRenamed("col1","Value")
            ///val df_melt_null = df_melt.filter(col("Value").isNull || col("Value").isNaN).withColumnRenamed("Value","bucket_range").withColumn("bucket_",lit("NullValue"))
            val bucketedData = bucketizeWithinPartition(df_melt, splits_map, "column_name", "Value", spark)
            bucketedData.persist(StorageLevel.MEMORY_ONLY_2)
            var bucketedData_range = bucketedData.na.fill("Missing").withColumn("bucket_range",concat(lit("Bucket_"),col("bucket_start"),lit("-"),col("bucket_end")))
            bucketedData_range = bucketedData_range.withColumn("bucket_range", regexp_replace(bucketedData_range("bucket_range"), "-2.147483647E9|-Missing", ""))
            bucketedData_range = bucketedData_range.withColumn("bucket_range", regexp_replace(bucketedData_range("bucket_range"), "null", "Bucket_Missing"))
            bucketedData_range = bucketedData_range.withColumn("bucket_range",when(col("bucket_range").isNull || col("bucket_range").isNaN ,lit("Bucket_Missing")).otherwise(col("bucket_range")))
            bucketedData_range.show(10,false)
            df_final = df_final :+ bucketedData_range
          }
          var bucketedData_transform = df_final.reduce(_ union _).groupBy(col("bucket"),col("column_name"),col("bucket_range")).count()
          ///var bucketedData_df_null = df_null_final.reduce(_ union _).groupBy(col("bucket"),col("column_name"),col("bucket_range")).count()
          bucketedData_transform = bucketedData_transform.withColumnRenamed("count","Metric_Abs_value")
          ///bucketedData_df_null = bucketedData_df_null.withColumnRenamed("count","Metric_Abs_value")

          val cols1 = bucketedData_transform.columns.toSet
          ///val cols2 = bucketedData_df_null.columns.toSet
          ///val total = cols1 ++ cols2 // union

          def expr(myCols: Set[String], allCols: Set[String]) = {
            allCols.toList.map(x => x match {
              case x if myCols.contains(x) => col(x)
              case _ => lit(null).as(x)
            })
          }
          ///bucketedData_final = bucketedData_transform.select(expr(cols1, total):_*).union(bucketedData_df_null.select(expr(cols2, total):_*)).orderBy(asc("column_name"))
          ///  .drop("bucket").withColumnRenamed("bucket_range","metric_name").withColumn("Metric_Ratio_value",bucketedData_transform("Metric_Abs_value")/count_df)
          bucketedData_transform = bucketedData_transform.orderBy(asc("column_name"))
            .drop("bucket")
            .withColumnRenamed("bucket_range","metric_name")
            .withColumn("Metric_Ratio_value",bucketedData_transform("Metric_Abs_value")/count_df)
          bucketedData_final = bucketedData_final :+ bucketedData_transform
        }
        case _ => {
          println("skip")
        }
      }}
    try {
      if(bucketedData_final.isEmpty){
        println("INFO: Transformation Util - object, build_category - method -- rule category doesn't match in Case statement, returning empty dataframe")
        return spark.createDataFrame(sc.emptyRDD[Row], bucketedData_final_schema)
      }
      else {
        return bucketedData_final.reduce(_ union _)
      }
    }
    catch {
      case e: Exception =>
        println("ERROR: Transformation Util - object, build_category - method -- rule category doesn't match in Case statement, returning empty dataframe")
        return spark.createDataFrame(sc.emptyRDD[Row], bucketedData_final_schema)
    }
  }


  //Create Methods to Apply on DQ DataFrame

  def getRule(constraint: String) = {

    if (constraint.contains("dataType")) "hasDataType"
    else if (constraint.contains("non-negative")) "isNonNegative"
    else if (constraint.contains("is positive")) "isPositive"
    else if (constraint.contains("Completeness")) "isComplete : => should never be NULL"
    else if (constraint.contains("Uniqueness")) "isUnique"
    else if (constraint.contains("MinLength")) "hasMinLength"
    else if (constraint.contains("MaxLength")) "hasMaxLength"
    else if (constraint.contains("Minimum") && constraint.contains("MinimumConstraint")) "hasMin"
    else if (constraint.contains("Maximum") && constraint.contains("MaximumConstraint")) "hasMax"
    else if (constraint.contains("Distinctness") && constraint.contains("DistinctnessConstraint")) "hasDistinctness"
    else if (constraint.contains("HistogramBinConstraint") && constraint.contains("Histogram")) "hasNumberOfDistinctValues"
    else if (constraint.contains("Size")) "hasSize"
    else if (constraint.contains("Compliance") && constraint.contains("contained in ")) "isContainedIn"
    else if (constraint.contains("Compliance") && !constraint.contains("contained in ")) "satisfies"
    else constraint

  }


  def getColumnName(constraint: String): String = {
    val replaceString = constraint.replaceAll("\\d", "").replaceAll("\\,,Some(TRUE)", "").replaceAll("COALESCE", "").replace("(CAST(", "(").replace(")", " ").replace(",<function1>", "").replaceAll(",Some", "")
      .replaceAll(s"(\\w+\\(){2}", "$1")
    val nextString_1 = replaceString.trim.split("\\(")(1)
    val nextString_2 = nextString_1.trim.replaceAll(", ", ",")
    val nextString_3 = nextString_2.trim.split(" ")(0)
    if (nextString_3.takeRight(1) == ",")
      nextString_3.dropRight(1)
    else nextString_3
  }


  def getFinalRuleName(temp_rule_name: String, Name: String): String = {
    if (Name == null || Name.isEmpty || Name.trim == "null")
      temp_rule_name
    else Name.replace("isComplete", "isComplete : => should never be NULL")
  }


  def getDQRule(Constraint_rule: String, validation_rule: String): String = {
    val replaceString = Constraint_rule //constraint.replaceAll("\\d", "").replaceAll("\\,,Some(TRUE)","").replaceAll("COALESCE","").replace("(CAST(","(").replace(")"," ").replace(",<function1>","").replaceAll(",Some","")
      .replaceAll(s"(\\w+\\(){2}", "$1")

    val replaceString_1 = replaceString.replaceFirst("\\(", "~~").split("~~")(1)

    val replaceString_2 = if (replaceString_1.contains(" & ")) replaceString.split(("&"))(1).split(("}"))(0).trim + "}"
    else replaceString_1.trim

    var replaceString_3 = if (!replaceString_2.contains("(")) replaceString_2.replaceAll("\\)", "")
    else replaceString_2

    val replaceString_4 = if (replaceString_3.takeRight(1) == """)""") replaceString_3.trim.dropRight(1)
    else replaceString_3

    val replaceString_5 = if (replaceString_4.contains("""))""")) replaceString_4.replaceFirst("\\)", "") //.dropRight(1)
    else replaceString_4

    replaceString_5
  }


  def getFinalValidationRule(validation_rule: String, dq_rules: String, rule_name: String): String = {
    val ruleName = if (rule_name.contains("isComplete")) "isComplete" else rule_name
    if (ruleName.trim == "hasDataType" && validation_rule == null)
      dq_rules
    else if (validation_rule == null || validation_rule.isEmpty || validation_rule.trim == "null")
      "name: " + ruleName + "{column: " + dq_rules + "}"
    else validation_rule.trim
  }


  def correctConstraintMessage(constraint_msg: String): String = {
    if (constraint_msg.contains("Value: ")) constraint_msg.replaceFirst("not", "".trim)
    else constraint_msg
  }


  def correctPassCount(result: String, total_rows: String, pass_count: String): String = {
    if (result.trim == "Success") total_rows
    else if (result.trim == "Failure") pass_count
    else ""
  }


  def correctFailCount(result: String, fail_count: String): String = {
    if (result.trim == "Success") "0.0"
    else if (result.trim == "Failure") fail_count
    else ""
  }


  val getColumnNameUdf = udf(getColumnName _)
  val getRuleNameUdf = udf(getRule _)
  val getFinalRule = udf(getFinalRuleName _)
  val getDQRuleUdf = udf(getDQRule _)
  val getFinalDQRuleDescription = udf(getFinalValidationRule _)
  val correctConsMsg = udf(correctConstraintMessage _)
  val getPassCount = udf(correctPassCount _)
  val getFailCount = udf(correctFailCount _)

}



