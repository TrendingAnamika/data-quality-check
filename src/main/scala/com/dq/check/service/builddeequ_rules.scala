package com.dq.check.service

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import org.apache.spark

import scala.util.matching.Regex

class builddeequ_rules {
  var checks = Check(CheckLevel.Warning, "Data unit test")
  var seq_id = 1
  val Pattern = "(<[=>]?|==|=<|=>|>=?|\\&\\&|\\|\\|)\\s+(.+)".r
  val numberOfRows: Long = Report_thread.count_df
  //val numberOfRows: Long = DataCheckImplementation.count_df

  def isComplete(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: isComplete{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.isComplete(columnName, hint = Some("Notnull condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.isComplete(columnName, hint = Some("Notnull condition should be satisfied & %s".format(uniqueID)))

  }

  def isUnique(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString

    val uniqueID = "name: isUnique{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.isUnique(columnName, hint = Some("Uniqueness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.isUnique(columnName, hint = Some("Uniqueness condition should be satisfied & %s".format(uniqueID)))

  }

  def isPositive(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: isPositive{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.isPositive(columnName, hint = Some("isPositive condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.isPositive(columnName, hint = Some("isPositive condition should be satisfied & %s".format(uniqueID)))
  }

  def isNonNegative(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: isNonNegative{column: " + columnName+ "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.isNonNegative(columnName, hint = Some("NonNegative condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.isNonNegative(columnName, hint = Some("NonNegative condition should be satisfied & %s".format(uniqueID)))
  }

  def containsSocialSecurityNumber(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: containsSocialSecurityNumber{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.containsSocialSecurityNumber(columnName, _ == 1.0, hint = Some("SocialSecurityNumber condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.containsSocialSecurityNumber(columnName, _ == 1.0, hint = Some("SocialSecurityNumber condition should be satisfied & %s".format(uniqueID)))

  }

  def containsCreditCardNumber(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: containsCreditCardNumber{column: " + columnName+ "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.containsCreditCardNumber(columnName, _ == 1.0, hint = Some("CreditCardNumber condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.containsCreditCardNumber(columnName, _ == 1.0, hint = Some("CreditCardNumber condition should be satisfied & %s".format(uniqueID)))
  }

  def containsURL(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: containsURL{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.containsURL(columnName, _ == 1.0, hint = Some("URL condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.containsURL(columnName, _ == 1.0, hint = Some("URL condition should be satisfied & %s".format(uniqueID)))
  }

  def containsEmail(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val columnName = arg("column").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: containsEmail{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.containsEmail(columnName, _ == 1.0, hint = Some("Email condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.containsEmail(columnName, _ == 1.0, hint = Some("Email condition should be satisfied & %s".format(uniqueID)))
  }

  def hasPattern(args: Map[String,Any], Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column")
    val regpattern = arg("Pattern")
    val where_cond = arg.getOrElse("where_cond","")
    val pattern = new Regex(regpattern)
    val result: String = arg.getOrElse("condition", "== 1")
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val uniqueID = "name: hasPattern{column: " + columnName + ",Pattern: " + pattern + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasPattern(columnName, pattern,_ >= chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasPattern(columnName, pattern,_ <= chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasPattern(columnName, pattern,_ == chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasPattern(columnName, pattern,_ > chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasPattern(columnName, pattern,_ < chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID))).where(where_cond)
      }}
    else {
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasPattern(columnName, pattern,_ >= chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasPattern(columnName, pattern,_ <= chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasPattern(columnName, pattern,_ == chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasPattern(columnName, pattern,_ > chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasPattern(columnName, pattern,_ < chk_val, hint = Some("Pattern should be satisfied & %s".format(uniqueID)))
      }
    }
  }

  def hasSize(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val uniqueID = "name: hasSize{ " + "condition: " + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasSize(_ >= chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasSize(_ <= chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasSize(_ == chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasSize(_ > chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasSize(_ < chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }} else
    {
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasSize(_ >= chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasSize(_ <= chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasSize(_ == chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasSize(_ > chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasSize(_ < chk_val, hint = Some("Size condition should be satisfied & %s".format(uniqueID)))
      }}
  }
  def hasSum(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column")
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","")
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasSum{column: " + columnName + ",Condition: " + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasSum(columnName,_ >= chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasSum(columnName,_ <= chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasSum(columnName, _ == chk_val, hint = Some("Sum should be equal to %f & %s".format(chk_val,uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasSum(columnName,_ > chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasSum(columnName,_ < chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      }} else    {
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasSum(columnName,_ >= chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasSum(columnName,_ <= chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID)))
        case "==" =>
          checks = checks.hasSum(columnName, _ == chk_val, hint = Some("Sum should be equal to %f & %s".format(chk_val,uniqueID)))
        case ">" =>
          checks = checks.hasSum(columnName,_ > chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID)))
        case "<" =>
          checks = checks.hasSum(columnName,_ < chk_val, hint = Some("Sum condition should be satisfied %s & %s".format(chk_val,uniqueID)))
      }}
  }

  def hasApproxCountDistinct(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column")
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","")
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasApproxCountDistinct{column: " + columnName + ",Condition: " + result  + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasApproxCountDistinct(columnName,_ >= chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasApproxCountDistinct(columnName,_ <= chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasApproxCountDistinct(columnName, _ == chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasApproxCountDistinct(columnName,_ > chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasApproxCountDistinct(columnName,_ < chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }} else {
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasApproxCountDistinct(columnName,_ >= chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasApproxCountDistinct(columnName,_ <= chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasApproxCountDistinct(columnName, _ == chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasApproxCountDistinct(columnName,_ > chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasApproxCountDistinct(columnName,_ < chk_val, hint = Some("ApproxCountDistinct condition should be satisfied & %s".format(uniqueID)))
      }}
  }
  def hasStandardDeviation(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column")
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","")
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasStandardDeviation{column: " + columnName + ",Condition: " + result  + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasStandardDeviation(columnName,_ >= chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasStandardDeviation(columnName,_ <= chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasStandardDeviation(columnName, _ == chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasStandardDeviation(columnName,_ > chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasStandardDeviation(columnName,_ < chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }} else {
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasStandardDeviation(columnName,_ >= chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasStandardDeviation(columnName,_ <= chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasStandardDeviation(columnName, _ == chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasStandardDeviation(columnName,_ > chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasStandardDeviation(columnName,_ < chk_val, hint = Some("StandardDeviation condition should be satisfied & %s".format(uniqueID)))
      }}
  }
  def hasMaxLength(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column")
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","TRUE")
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasMaxLength{column: " + columnName + ",Condition: " + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    condition match {
      case ">=" | "=>"  =>
        checks = checks.hasMaxLength(columnName,_ >= chk_val, hint = Some("MaxLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "<=" | "=<" =>
        checks = checks.hasMaxLength(columnName,_ <= chk_val, hint = Some("MaxLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "==" =>
        checks = checks.hasMaxLength(columnName, _ == chk_val, hint = Some("MaxLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case ">" =>
        checks = checks.hasMaxLength(columnName,_ > chk_val, hint = Some("MaxLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "<" =>
        checks = checks.hasMaxLength(columnName,_ < chk_val, hint = Some("MaxLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
    }}

  def hasMinLength(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column").toString
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","TRUE").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasMinLength{column: " + columnName + ",Condition: " + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    condition match {
      case ">=" | "=>"  =>
        checks = checks.hasMinLength(columnName,_ >= chk_val, hint = Some("MinLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "<=" | "=<" =>
        checks = checks.hasMinLength(columnName,_ <= chk_val, hint = Some("MinLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "==" =>
        checks = checks.hasMinLength(columnName, _ == chk_val, hint = Some("MinLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case ">" =>
        checks = checks.hasMinLength(columnName,_ > chk_val, hint = Some("MinLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "<" =>
        checks = checks.hasMinLength(columnName,_ < chk_val, hint = Some("MinLength condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
    }}

  def hasNumberOfDistinctValues(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column").toString
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","TRUE").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasNumberOfDistinctValues{column: " + columnName + ",Condition: " + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    println(condition)
    condition match {
      case ">=" | "=>"  =>
        checks = checks.hasNumberOfDistinctValues(columnName,_ >= chk_val, hint = Some("NumberOfDistinctValues condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "<=" | "=<" =>
        checks = checks.hasNumberOfDistinctValues(columnName,_ <= chk_val, hint = Some("NumberOfDistinctValues condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "==" =>
        checks = checks.hasNumberOfDistinctValues(columnName, _ == chk_val, hint = Some("NumberOfDistinctValues condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case ">" =>
        checks = checks.hasNumberOfDistinctValues(columnName,_ > chk_val, hint = Some("NumberOfDistinctValues condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
      case "<" =>
        checks = checks.hasNumberOfDistinctValues(columnName,_ < chk_val, hint = Some("NumberOfDistinctValues condition should be satisfied %s & %s".format(chk_val,uniqueID))).where(where_cond)
    }}
  def isContainedIn(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val columnType = arg("datatype").toString.toLowerCase
    val where_cond = arg.getOrElse("where_cond","").toString
    columnType match {
      case "string" => {
        val list = arg("list").asInstanceOf[List[String]].toArray
        val uniqueID = "name: isContainedIn{column: " + columnName + "_list: " + list.mkString(",") + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
        seq_id += 1
        if(where_cond != "")
          checks = checks.isContainedIn(columnName, list, hint = Some("isContainedIn should be equal to %s &%s".format(list, uniqueID))).where(where_cond)
        else
          checks = checks.isContainedIn(columnName, list, hint = Some("isContainedIn should be equal to %s &%s".format(list, uniqueID)))
      }
      case "double" => {
        val list = arg("list").asInstanceOf[List[Double]]
        val lowerBound = list(0)
        val upperBound = list(1)
        val uniqueID = "name: isContainedIn{column: " + columnName + "_Values: " + List(lowerBound, upperBound) + "|" + seq_id
        seq_id += 1
        if(where_cond != "")
          checks = checks.isContainedIn(columnName, lowerBound, upperBound, hint = Some("isContainedIn should be between %f and %f & %s".format(lowerBound, upperBound, uniqueID))).where(where_cond)
        else
          checks = checks.isContainedIn(columnName, lowerBound, upperBound, hint = Some("isContainedIn should be between %f and %f & %s".format(lowerBound, upperBound, uniqueID)))
      }
    }
  }

  def hasUniqueness(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val list = arg("column").asInstanceOf[Seq[String]]
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: hasUniqueness{column: " + list.mkString(",") + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.hasUniqueness(list, Check.IsOne, hint = Some("Uniqueness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.hasUniqueness(list, Check.IsOne, hint = Some("Uniqueness condition should be satisfied & %s".format(uniqueID)))
  }

  def isPrimaryKey(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: isPrimaryKey{column: " + list.mkString(",") + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != "")
      checks = checks.isPrimaryKey(list.mkString(","), hint = Some("PrimaryKey condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    else
      checks = checks.isPrimaryKey(list.mkString(","), hint = Some("PrimaryKey condition should be satisfied & %s".format(uniqueID)))

  }
  def hasDistinctness(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String,Any]]
    val list = arg("column").asInstanceOf[Seq[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","").toString

    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasDistinctness{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasDistinctness(list,_ >= chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasDistinctness(list,_ <= chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasDistinctness(list, _ == chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasDistinctness(list,_ > chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasDistinctness(list,_ < chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }}
    else{
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasDistinctness(list,_ >= chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasDistinctness(list,_ <= chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasDistinctness(list, _ == chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasDistinctness(list,_ > chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasDistinctness(list,_ < chk_val, hint = Some("Distinctness condition should be satisfied & %s".format(uniqueID)))
      }}
  }
  def hasCompleteness(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column").toString
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasCompleteness{column: " + columnName + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasCompleteness(columnName,_ >= chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasCompleteness(columnName,_ <= chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasCompleteness(columnName, _ == chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasCompleteness(columnName,_ > chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasCompleteness(columnName,_ < chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }}
    else{
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasCompleteness(columnName,_ >= chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasCompleteness(columnName,_ <= chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasCompleteness(columnName, _ == chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasCompleteness(columnName,_ > chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasCompleteness(columnName,_ < chk_val, hint = Some("Completeness condition should be satisfied & %s".format(uniqueID)))
      }}
  }
  def hasEntropy(args: Any, Failure_Flag:String)  {
    val arg = args.asInstanceOf[Map[String,String]]
    val columnName = arg("column").toString
    val result =  arg("condition")
    val where_cond = arg.getOrElse("where_cond","").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasEntropy{column: " + columnName + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasEntropy(columnName,_ >= chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasEntropy(columnName,_ <= chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasEntropy(columnName, _ == chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasEntropy(columnName,_ > chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasEntropy(columnName,_ < chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }}
    else{
      condition match {
        case ">=" | "=>"  =>
          checks = checks.hasEntropy(columnName,_ >= chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasEntropy(columnName,_ <= chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasEntropy(columnName, _ == chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasEntropy(columnName,_ > chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasEntropy(columnName,_ < chk_val, hint = Some("Entropy condition should be satisfied & %s".format(uniqueID)))
      }}
  }

  def isLessThan(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","TRUE").toString

    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val col1 = list(0)
    val col2 = list(1)
    val uniqueID = "name: isLessThan{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    condition match {
      case ">=" | "=>" =>
        checks = checks.isLessThan(col1, col2, _ >= chk_val, hint = Some("LessThan condition should be satisfied & %s".format(uniqueID)))
      case "<=" | "=<" =>
        checks = checks.isLessThan(col1, col2, _ <= chk_val, hint = Some("LessThan condition should be satisfied & %s".format(uniqueID)))
      case "==" =>
        checks = checks.isLessThan(col1, col2, _ == chk_val, hint = Some("LessThan condition should be satisfied & %s".format(uniqueID)))
      case ">" =>
        checks = checks.isLessThan(col1, col2, _ > chk_val, hint = Some("LessThan condition should be satisfied & %s".format(uniqueID)))
      case "<" =>
        checks = checks.isLessThan(col1, col2, _ < chk_val, hint = Some("LessThan condition should be satisfied & %s".format(uniqueID)))
    }
  }
  def isLessThanOrEqualTo(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","TRUE").toString

    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val col1 = list(0)
    val col2 = list(1)
    val uniqueID = "name: isLessThanOrEqualTo{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>" =>
          checks = checks.isLessThanOrEqualTo(col1, col2, _ >= chk_val, hint = Some("LessThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.isLessThanOrEqualTo(col1, col2, _ <= chk_val, hint = Some("LessThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.isLessThanOrEqualTo(col1, col2, _ == chk_val, hint = Some("LessThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.isLessThanOrEqualTo(col1, col2, _ > chk_val, hint = Some("LessThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.isLessThanOrEqualTo(col1, col2, _ < chk_val, hint = Some("LessThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }}
  }
  def isGreaterThan(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","TRUE").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val col1 = list(0)
    val col2 = list(1)
    val uniqueID = "name: isGreaterThan{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    condition match {
      case ">=" | "=>" =>
        checks = checks.isGreaterThan(col1, col2, _ >= chk_val, hint = Some("GreaterThan condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case "<=" | "=<" =>
        checks = checks.isGreaterThan(col1, col2, _ <= chk_val, hint = Some("GreaterThan condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case "==" =>
        checks = checks.isGreaterThan(col1, col2, _ == chk_val, hint = Some("GreaterThan condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case ">" =>
        checks = checks.isGreaterThan(col1, col2, _ > chk_val, hint = Some("GreaterThan condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case "<" =>
        checks = checks.isGreaterThan(col1, col2, _ < chk_val, hint = Some("GreaterThan condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    }
  }
  def isGreaterThanOrEqualTo(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","TRUE").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val col1 = list(0)
    val col2 = list(1)
    val uniqueID = "name: isGreaterThanOrEqualTo{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    condition match {
      case ">=" | "=>" =>
        checks = checks.isGreaterThanOrEqualTo(col1, col2, _ >= chk_val, hint = Some("GreaterThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case "<=" | "=<" =>
        checks = checks.isGreaterThanOrEqualTo(col1, col2, _ <= chk_val, hint = Some("GreaterThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case "==" =>
        checks = checks.isGreaterThanOrEqualTo(col1, col2, _ == chk_val, hint = Some("GreaterThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case ">" =>
        checks = checks.isGreaterThanOrEqualTo(col1, col2, _ > chk_val, hint = Some("GreaterThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      case "<" =>
        checks = checks.isGreaterThanOrEqualTo(col1, col2, _ < chk_val, hint = Some("GreaterThanOrEqualTo condition should be satisfied & %s".format(uniqueID))).where(where_cond)
    }
  }
  def hasUniqueValueRatio(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[Seq[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","").toString

    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val uniqueID = "name: hasUniqueValueRatio{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>" =>
          checks = checks.hasUniqueValueRatio(list, _ >= chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasUniqueValueRatio(list,_ <= chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasUniqueValueRatio(list, _ == chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasUniqueValueRatio(list, _ > chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasUniqueValueRatio(list, _ < chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      }}
    else{
      condition match {
        case ">=" | "=>" =>
          checks = checks.hasUniqueValueRatio(list, _ >= chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasUniqueValueRatio(list,_ <= chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.hasUniqueValueRatio(list, _ == chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.hasUniqueValueRatio(list, _ > chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.hasUniqueValueRatio(list, _ < chk_val, hint = Some("UniqueValueRatio condition should be satisfied & %s".format(uniqueID)))
      }
    }
  }
  def hasMutualInformation(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","").toString

    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val col1 = list(0)
    val col2 = list(1)
    val uniqueID = "name: hasMutualInformation{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>" =>
          checks = checks.hasMutualInformation(col1, col2, _ >= chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasMutualInformation(col1, col2, _ <= chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasMutualInformation(col1, col2, _ == chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasMutualInformation(col1, col2, _ > chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasMutualInformation(col1, col2, _ < chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID))).where(where_cond)
      }}
    else{
      condition match {
        case ">=" | "=>" =>
          checks = checks.hasMutualInformation(col1, col2, _ >= chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasMutualInformation(col1, col2, _ <= chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID)))
        case "==" =>
          checks = checks.hasMutualInformation(col1, col2, _ == chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID)))
        case ">" =>
          checks = checks.hasMutualInformation(col1, col2, _ > chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID)))
        case "<" =>
          checks = checks.hasMutualInformation(col1, col2, _ < chk_val, hint = Some("MutualInformation condition should be satisfied %s & %s".format(list, uniqueID)))
      }
    }
  }
  def hasCorrelation(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val list = arg("column").asInstanceOf[List[String]]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    if (chk_val_raw.toLowerCase.contains("/numberofrows")) {
      val str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val col1 = list(0)
    val col2 = list(1)
    val uniqueID = "name: hasCorrelation{column: " + list.mkString(",") + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>" =>
          checks = checks.hasCorrelation(col1, col2, _ >= chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.hasCorrelation(col1, col2, _ <= chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.hasCorrelation(col1, col2, _ == chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.hasCorrelation(col1, col2, _ > chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.hasCorrelation(col1, col2, _ < chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID))).where(where_cond)
      }}
    else{
      condition match {
        case ">=" | "=>" =>
          checks = checks.hasCorrelation(col1, col2, _ >= chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID)))
        case "<=" | "=<" =>
          checks = checks.hasCorrelation(col1, col2, _ <= chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID)))
        case "==" =>
          checks = checks.hasCorrelation(col1, col2, _ == chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID)))
        case ">" =>
          checks = checks.hasCorrelation(col1, col2, _ > chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID)))
        case "<" =>
          checks = checks.hasCorrelation(col1, col2, _ < chk_val, hint = Some("Correlation condition should be satisfied %s & %s".format(list,uniqueID)))
      }
    }
  }
  def hasMin(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val lowerBound = arg.get("lowerBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val upperBound = arg.get("upperBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val where_cond = arg.getOrElse("where_cond","").toString
    val uniqueID = "name: hasMin{column: " + columnName + "_range: " + List(lowerBound,upperBound) + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if (lowerBound != Double.NaN) {
      if(where_cond != "")
        checks = checks.hasMin(columnName, _ >= lowerBound, hint = Some("Minimum value should be greater than or equal to %f & %s".format(lowerBound,uniqueID))).where(where_cond)
      else
        checks = checks.hasMin(columnName, _ >= lowerBound, hint = Some("Minimum value should be greater than or equal to %f & %s".format(lowerBound,uniqueID)))
    }
    if (upperBound != Double.NaN) {
      if(where_cond != "")
        checks = checks.hasMin(columnName, _ <= upperBound, hint = Some("Minimum value should be less than or equal to %f & %s".format(upperBound,uniqueID))).where(where_cond)
      else
        checks = checks.hasMin(columnName, _ <= upperBound, hint = Some("Minimum value should be less than or equal to %f & %s".format(upperBound,uniqueID)))
    }
  }
  def hasApproxQuantile(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val quantitle = arg.get("quantitle").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","TRUE").toString
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    chk_val = chk_val_raw.toDouble

    val uniqueID = "name: hasApproxQuantile{column: " + columnName + "_range: " + List(quantitle,result) + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    condition match {
      case ">=" | "=>" =>
        checks = checks.hasApproxQuantile(columnName, quantitle, _ >= chk_val, hint = Some("ApproxQuantile condition should be satisfied %s & %s".format(result,uniqueID))).where(where_cond)
      case "<=" | "=<" =>
        checks = checks.hasApproxQuantile(columnName, quantitle, _ <= chk_val, hint = Some("ApproxQuantile condition should be satisfied %s & %s".format(result,uniqueID))).where(where_cond)
      case "==" =>
        checks = checks.hasApproxQuantile(columnName, quantitle, _ == chk_val, hint = Some("ApproxQuantile condition should be satisfied %s & %s".format(result,uniqueID))).where(where_cond)
      case ">" =>
        checks = checks.hasApproxQuantile(columnName, quantitle, _ > chk_val, hint = Some("ApproxQuantile condition should be satisfied %s & %s".format(result,uniqueID))).where(where_cond)
      case "<" =>
        checks = checks.hasApproxQuantile(columnName, quantitle, _ <= chk_val, hint = Some("ApproxQuantile condition should be satisfied %f & %s".format(result,uniqueID))).where(where_cond)
    }
  }
  def hasMean(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val lowerBound = arg.get("lowerBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val upperBound = arg.get("upperBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val where_cond = arg.getOrElse("where_cond","").toString

    val uniqueID = "name: hasMean{column: " + columnName + "_range: " + List(lowerBound,upperBound) + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      if (lowerBound != Double.NaN) {
        checks = checks.hasMean(columnName, _ >= lowerBound, hint = Some("Average value should be greater than or equal to %f & %s".format(lowerBound,uniqueID))).where(where_cond)
      }
      if (upperBound != Double.NaN) {
        checks = checks.hasMean(columnName, _ <= upperBound, hint = Some("Average value should be less than or equal to %f & %s".format(upperBound,uniqueID))).where(where_cond)
      }}
    else{
      if (lowerBound != Double.NaN) {
        checks = checks.hasMean(columnName, _ >= lowerBound, hint = Some("Average value should be greater than or equal to %f & %s".format(lowerBound,uniqueID)))
      }
      if (upperBound != Double.NaN) {
        checks = checks.hasMean(columnName, _ <= upperBound, hint = Some("Average value should be less than or equal to %f & %s".format(upperBound,uniqueID)))
      }
    }
  }

  def hasHistogramValues(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val lowerBound = arg.get("lowerBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val upperBound = arg.get("upperBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val valuetype = arg.get("valuetype").orElse(Some("ratio")).get.asInstanceOf[String]
    val attribute = arg.get("attribute").orElse(Some(Double.NaN)).get.asInstanceOf[String]
    val where_cond = arg.getOrElse("where_cond","").toString

    val uniqueID = "name: hasHistogramValues{column: " + columnName + "_range: " + List(attribute,lowerBound,upperBound) + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      if (lowerBound != Double.NaN) {
        if (valuetype == "absolute") {
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).absolute > lowerBound, hint = Some("Percentage should be greater than or equal to %f & %s".format(lowerBound, uniqueID))).where(where_cond)
        }
        if (valuetype == "ratio"){
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).ratio >= lowerBound, hint = Some("Percentage should be greater than or equal to %f & %s".format(lowerBound,uniqueID))).where(where_cond)
        }}
      if (upperBound != Double.NaN) {
        if (valuetype == "absolute") {
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).absolute < upperBound, hint = Some("Percentage should be less than or equal to %f & %s".format(upperBound, uniqueID))).where(where_cond)
        }
        if (valuetype == "ratio") {
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).ratio < upperBound, hint = Some("Percentage should be less than or equal to %f & %s".format(upperBound, uniqueID))).where(where_cond)
        }
      }}
    else{
      if (lowerBound != Double.NaN) {
        if (valuetype == "absolute") {
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).absolute > lowerBound, hint = Some("Percentage should be greater than or equal to %f & %s".format(lowerBound, uniqueID)))
        }
        if (valuetype == "ratio"){
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).ratio > lowerBound, hint = Some("Percentage should be greater than or equal to %f & %s".format(lowerBound,uniqueID)))
        }}
      if (upperBound != Double.NaN) {
        if (valuetype == "absolute") {
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).absolute < upperBound, hint = Some("Percentage should be less than or equal to %f & %s".format(upperBound, uniqueID)))
        }
        if (valuetype == "ratio") {
          checks = checks.hasHistogramValues(columnName, _ (attribute.toString()).ratio <= upperBound, hint = Some("Percentage should be less than or equal to %f & %s".format(upperBound, uniqueID)))
        }
      }
    }
  }

  def hasMax(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val lowerBound = arg.get("lowerBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val upperBound = arg.get("upperBound").orElse(Some(Double.NaN)).get.asInstanceOf[Double]
    val where_cond = arg.getOrElse("where_cond","").toString

    val uniqueID = "name: hasMax{column: " + columnName + "_range: " + List(lowerBound,upperBound) + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if (lowerBound != Double.NaN) {
      if(where_cond != "")
        checks = checks.hasMax(columnName, _ >= lowerBound, hint = Some("Max value should be greater than or equal to %f & %s".format(lowerBound,uniqueID))).where(where_cond)
      else
        checks = checks.hasMax(columnName, _ >= lowerBound, hint = Some("Max value should be greater than or equal to %f & %s".format(lowerBound,uniqueID)))
    }
    if (upperBound != Double.NaN) {
      if(where_cond != "")
        checks = checks.hasMax(columnName, _ <= upperBound, hint = Some("Max value should be less than or equal to %f & %s".format(upperBound,uniqueID))).where(where_cond)
      else
        checks = checks.hasMax(columnName, _ <= upperBound, hint = Some("Max value should be less than or equal to %f & %s".format(upperBound,uniqueID)))
    }
  }
  def hasDataType(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val datatype = arg("datatype").toString.toLowerCase
    val where_cond = arg.getOrElse("where_cond","TRUE").toString
    val uniqueID = "name: hasDataType{column: " + columnName + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    datatype match {
      case "integer" =>
        checks = checks.hasDataType(columnName, ConstrainableDataTypes.Integral, hint = Some("Integer dataType condition should be satisfied & %s".format(uniqueID)))
      case "string" =>
        checks = checks.hasDataType(columnName, ConstrainableDataTypes.String, hint = Some("String dataType condition should be satisfied & %s".format(uniqueID)))
      case "boolean" =>
        checks = checks.hasDataType(columnName, ConstrainableDataTypes.Boolean, hint = Some("Boolean dataType condition should be satisfied & %s".format(uniqueID)))
      case "null" =>
        checks = checks.hasDataType(columnName, ConstrainableDataTypes.Null, hint = Some("Null dataType condition should be satisfied & %s".format(uniqueID)))
      case "numeric" =>
        checks = checks.hasDataType(columnName, ConstrainableDataTypes.Numeric, hint = Some("Numeric dataType condition should be satisfied & %s".format(uniqueID)))
      case "fractional" =>
        checks = checks.hasDataType(columnName, ConstrainableDataTypes.Fractional, hint = Some("Fractional dataType condition should be satisfied & %s".format(uniqueID)))
    }
  }
  def satisfies(args: Any, Failure_Flag:String) {
    val arg = args.asInstanceOf[Map[String, Any]]
    val columnName = arg("column").toString
    val result =  arg("condition").toString
    val where_cond = arg.getOrElse("where_cond","").toString
    var str_split = ""
    val Pattern(condition,chk_val_raw) = result
    var chk_val = 0.0
    println("chk_val_raw:" + chk_val_raw)
    if (chk_val_raw.toLowerCase.contains("numberofrows")) {
      str_split = chk_val_raw.toLowerCase.replaceAll("/numberofrows", "")
      chk_val =  str_split.toFloat/numberOfRows
    }
    else{
      chk_val = chk_val_raw.toDouble
    }
    val uniqueID = "name: satisfies{column: " + columnName + ",Condition:" + result + "} |" + seq_id + "& Failure_Flag: " + Failure_Flag
    seq_id += 1
    if(where_cond != ""){
      condition match {
        case ">=" | "=>" =>
          checks = checks.satisfies(columnName, "", _ >= chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<=" | "=<" =>
          checks = checks.satisfies(columnName, "", _ <= chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "==" =>
          checks = checks.satisfies(columnName, "", _ == chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case ">" =>
          checks = checks.satisfies(columnName, "", _ > chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID))).where(where_cond)
        case "<" =>
          checks = checks.satisfies(columnName, "", _ < chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID))).where(where_cond)
      } }
    else{
      condition match {
        case ">=" | "=>" =>
          checks = checks.satisfies(columnName, "", _ >= chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID)))
        case "<=" | "=<" =>
          checks = checks.satisfies(columnName, "", _ <= chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID)))
        case "==" =>
          checks = checks.satisfies(columnName, "", _ == chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID)))
        case ">" =>
          checks = checks.satisfies(columnName, "", _ > chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID)))
        case "<" =>
          checks = checks.satisfies(columnName, "", _ < chk_val, hint = Some("Satisfies condition should be satisfied & %s".format(uniqueID)))
      }
    }
  }
}

