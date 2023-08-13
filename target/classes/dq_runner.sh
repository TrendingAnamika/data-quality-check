#!/bin/bash

set -x

while [ "$1" != "" ]; do
    case $1 in
        -e | --environment )
                        shift
						envrnmt=$1
            ;;

        -c | --config )
                        shift
                        user_config_file=$1
            ;;

        -f | --filename )
                        shift
                        input_filename=$1
                        ;;


        -d | --loaddate )
                        shift
                        process_date=$1
            ;;

		-m | --executionmode )
                        shift
                        exec_mode=$1
            ;;

        * )
            other_args="${other_args} $1"
    esac
    shift
done


if [[ $exec_mode == "" ]] || [[ $envrnmt == "" ]] || [[ $user_config_file == "" ]] || ([[ $envrnmt != "dev" ]] && [[ $envrnmt != "uat" ]] && [[ $envrnmt != "prod" ]]); then
        echo "Error- Script should contain a minimum of three arguments with value dev / prod, execution mode and configuration file. Please check and re-run."
        echo "./dq_runner_2.0.sh -e <execution environment> -c <user configuration file> [-f <hdfs input file location>] [-d <historic load date for hive sql>]"
        echo "Ex: ./dq_runner_2.0.sh -e uat -m dq -c ./user.conf -f hdfs://node/user/abcduser/dataquality/ascii_esears41.dat -d 2020-01-01"
		#sh /path/data/latest/CRS-Data-Engineering/eap-crs-grmas-dq/build_directory/Common/scripts/dq_runner_2.0.sh -c  /path/data/latest/CRS-Data-Engineering/eap-crs-grmas-dq/src/main/resources/conf/test_db.tble.bbrpl1.conf -e prod -m dq
        exit 1
fi


#Will be substituted in Hive query
if [ -z "$process_date" ]; then
        #Set default current date, if user has not provided any historic date for backdated processing.
        process_date=$(date +"%Y-%m-%d")
fi


#Initialize Kerberos
kinit -k -t /opt/Cloudera/keytabs/`whoami`.`hostname -s`.keytab `whoami`/`hostname -f`@${hive_dns}


#----------Read configuration variables------------

#path="${BASH_SOURCE[0]}"
#frwrk_config_file=`echo ${path/scripts\/dq_runner_2.0.sh/config\/framework_2.0.conf}`
#spark_jar_file=`echo ${path/dq_runner_2.0.sh/Data_quality_framework-2.0-jar-with-dependencies.jar}`

#---Details from user configuration files-----------------
spark_conf=`cat "$user_config_file" | grep "spark_conf_$envrnmt" | sed s/"spark_conf_${envrnmt}="// | sed -r 's/\"//g' | xargs`
job_name=`cat "$user_config_file" | grep "jobApplicationName" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
#rules_json_file=`cat "$user_config_file" | grep "DQ_Rules_File" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
sql_file=`cat "$user_config_file" | grep "Dataset_Input_Query_$envrnmt" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
frwrk_config_file=`cat "$user_config_file" | grep "frwrk_config_file" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
spark_jar_file=`cat "$user_config_file" | grep "spark_jar_file" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
ipSchemaPath=`cat "$user_config_file" | grep "Dataset_Custom_Schema" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`

#cons_fail_mail_sub=`cat "$frwrk_config_file" | grep "cons_fail_mail_sub_$envrnmt" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`


#Check the existence of provided configuration file
#The frame work configuration file should reside in the same folder as DQ jar resides. Spark AM container will fail otherwise.

if [ ! -f "$user_config_file" ] || [ ! -f "$frwrk_config_file" ]; then
    err_msg="Error:- Provided configuration file does not exist($user_config_file) or ($frwrk_config_file). Please check. Exiting the script"
        echo "$err_msg"
        #err_email "$err_msg"
        exit 1
fi

#Pick the right rules JSON file
    case $exec_mode in
        dq )
                        rules_json_file=`cat "$user_config_file" | grep "DQ_Rules_File_$envrnmt" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
                        if [ ! -f "$rules_json_file" ]; then
                        	err_msg="Error:- Provided DQ JSON rules file does not exist ($rules_json_file). Please check 'DQ_Rules_File' value in $user_config_file file. Exiting the script"
        					echo "$err_msg"
                            exit 1
                        fi
            ;;

        profile )
                        rules_json_file=`cat "$user_config_file" | grep "Profiling_Records_$envrnmt" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
                        if [ ! -f "$rules_json_file" ]; then
                        	err_msg="Error:- Provided Profiling Records JSON rules file does not exist ($rules_json_file). Please check 'Profiling_Records' value in $user_config_file file. Exiting the script"
        					echo "$err_msg"
                            exit 1
                        fi
            ;;

        trnd_anlys )
                        rules_json_file=`cat "$user_config_file" | grep "Historical_Records_$envrnmt" | cut -d"=" -f2 | sed -r 's/\"//g' | xargs`
                        if [ ! -f "$rules_json_file" ]; then
                        	err_msg="Error:- Provided Historical Records JSON rules file does not exist ($rules_json_file). Please check 'Historical_Records' value in $user_config_file file. Exiting the script"
        					echo "$err_msg"
                            exit 1
                        fi
            ;;
    esac



if [[ $(fgrep -ix "$Dataset_Type" <<< "hive") ]] && [ ! -f "$dq_IpHiveQuery" ]; then
    err_msg="Error:- In $user_config_file file, the (Dataset_Type) is defined as Hive but corresponding SQL file is not specified in dq_IpHiveQuery. Please check. Exiting the script"
        echo "$err_msg"
        #err_email "$err_msg"
        exit 1
fi


if [[ $(fgrep -ix "$Dataset_Infer_Schema" <<< "custom") ]] && [ ! -f "$ipSchemaPath" ]; then
    err_msg="Error:- In $user_config_file file, the (Dataset_Infer_Schema) is defined as custom but corresponding json schema file is not specified in Dataset_Custom_Schema. Please check. Exiting the script"
        echo "$err_msg"
        #err_email "$err_msg"
        exit 1
fi


if ([[ $(fgrep -ix "$Dataset_Type" <<< "parquet") ]] || [[ $(fgrep -ix "$Dataset_Type" <<< "csv") ]] || [[ $(fgrep -ix "$Dataset_Type" <<< "json") ]]) && [[ "$input_filename" == "" ]]; then
    err_msg="Error:- In $user_config_file file, the (Dataset_Type) is defined as parquet or csv or json but corresponding hdfs input file is not specified while triggering the script. Please check. Exiting the script"
        echo "$err_msg"
        #err_email "$err_msg"
        exit 1
fi

#Will be used if user does a file based load for data validation
if [ -z "$input_filename" ]; then
        #Set to NA if value is not provided. If this is empty, it may fail the spark job with Array index out of bound exception.
        input_filename="NA"
fi

#Submit spark job to execute data quality validation
if [[ $envrnmt == "dev" ]]; then
spark-submit ${spark_conf} --name ${job_name} --files ${frwrk_config_file},${user_config_file},${rules_json_file},${sql_file},${ipSchemaPath} --master yarn --deploy-mode cluster --class com.wfs.dq.DataCheckImplementation ${spark_jar_file} ip_file=${input_filename} env=${envrnmt} user_config=${user_config_file} exec_mode=${exec_mode} process_date=${process_date} ${other_args}
elif [[ $envrnmt == "uat" ]] || [[ $envrnmt == "prod" ]]; then
spark2-submit ${spark_conf} --name ${job_name} --files ${frwrk_config_file},${user_config_file},${rules_json_file},${sql_file},${ipSchemaPath} --master yarn --deploy-mode cluster --class com.wfs.dq.DataCheckImplementation ${spark_jar_file} ip_file=${input_filename} env=${envrnmt} user_config=${user_config_file} exec_mode=${exec_mode} process_date=${process_date} ${other_args}
fi


#After Completing Data Quality Spark Job :

curr_dir=`pwd`

cd `/apps/analytics/etrading/dq/test/`

rm -r DQ_*

hadoop fs -getmerge /etrading/fx/test/*.csv /apps/analytics/etrading/dq/test/DQ_Analysis.csv

hadoop fs -rm -r /etrading/fx/test/*

awk '{if(!seen[$0]++)print $0}' /apps/analytics/etrading/dq/test/DQ_Analysis.csv > /apps/analytics/etrading/dq/test/DQ_Test_Analysis.csv

rm -r /apps/analytics/etrading/dq/test/DQ_Analysis.csv



echo "Please check the DQ_Analysis.csv file for report."



#Add for email sendind here



ret_val=$?

if [[ $ret_val -ne 0 ]]; then

        echo "Spark job didn't complete successfully. Exiting the wrapper script with error."

        exit 1

fi




