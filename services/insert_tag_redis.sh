#!/bin/bash

REMOTE_APP_DATA_PATH=/data/zyz/services/uds/app/data/
REMOTE_PC_DATA_PATH=/data/zyz/services/uds/pc/data/
UDS_DATA_PATH=/opt/zyz/services/uds_spark/tara/
APP_DATA_PATH=/opt/zyz/services/uds_spark/data/
HIVE_UDF_PATH=hdfs://namenodeha/hive/udf

APP_REDIS_NUM=10
PC_REDIS_NUM=10

if [ $# -ge 1 ];
then
    DATE=$1
    typeset -l mode
    mode=$2
    test=`echo $DATE | grep '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'`
    if [ "$test" == "" ]; then
        echo "Invalid tara!"
        exit -1
    fi
else
    echo "Error:Usage: sh -x *.sh date [whole|incr]"
    exit
fi

logfile=/opt/uds/logs/redis_data.log

function log_debug()
{
    echo "`date +"%Y-%m-%d %H:%M:%S"` [DEBUG] ($$): $*" ;
    echo "`date +"%Y-%m-%d %H:%M:%S"` [DEBUG] ($$): $*" >> $logfile;
}

mail_list=shitong@optaim.com,fuzhihao@optaim.com,cuiyulei@optaim.com,yangshuo@optaim.com

function log_error()
{
    echo "`date +"%Y-%m-%d %H:%M:%S"` [ERROR] ($$): $*" ;
    echo "`date +"%Y-%m-%d %H:%M:%S"` [ERROR] ($$): $*" >> $logfile;
    echo "$*"|mail -s "UDS TAGGER ERROR" $MAIL_LIST
}

###### 工具方法：条件运行function #######
function condition_run_method()
{
    isRun=$1
    method=$2
    flag=1
    if [ $isRun -eq 0 ]
    then
        $2
        flag=$?
        if [ $flag -eq 0 ]
        then
            log_debug "run $method succeed"
        else
            log_error "run $method failed"
        fi
    fi
    return $flag
}

function cp_files_to_remote_with_retry()
{
    src=$1
    dst=$2
    retryTimes=3
    for i in `seq $retryTimes`
    do
      sleep 30
      rsync -avzP --bwlimit=10000 $src $dst&&break
    done
}

function save_whole_app_redis_table()
{
  TABLE_FROM=$1
  TABLE_TO=$2
  REDUCE_NUM=$3

  # 'limit 700000000' ==> one redis have max 70000000 rows,or it will beyond the memory limit
  #
  # change {setex key ttl value }
  #        to redis protocol -->
  #        {*4 \r\n $5 \r\n setex \r\n $length(key) \r\n key \r\n $length(ttl) \r\n ttl \r\n $length(value) \r\n value}
  # example {set b1416e23ff4385668e36639c169c67ca 388800 1100} ==> {*4 \r\n $5 \r\n setex \r\n $32 \r\n b1416e23ff4385668e36639c169c67ca \r\n $6 \r\n 388800 $4 \r\n 1100}
  hive <<EOF
          add jar $HIVE_UDF_PATH/zyz_udf2.jar;
          create temporary function crc32  as 'org.apache.hadoop.hive.ql.udf.CRC32_UDF';
          set hive.exec.dynamic.partition.mode=nonstrict;
          SET hive.exec.compress.output=true;
          SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
          set mapreduce.job.reduces=$REDUCE_NUM;
          truncate TABLE $TABLE_TO;
          INSERT overwrite TABLE $TABLE_TO partition(partition_id)
          SELECT concat(concat_ws('\u000d\n','*4','\$5','setex','\$32',zuid,'\$7','3888000',concat('$',length(concat_ws(',',tags))),concat_ws(',',tags)),'\u000d') kv,
                 crc32(zuid,'$APP_REDIS_NUM') partition_id
                      FROM
                      (
                        SELECT zuid,tags from $TABLE_FROM 
                             WHERE day_id='$DATE'
                             AND length(zuid)=32
                             AND size(tags)>0
                             AND updtime>date_sub('$DATE',30)
                             AND (credit<50 or credit>97)
                      ) a
           group BY crc32(zuid,'$APP_REDIS_NUM'),concat(concat_ws('\u000d\n','*4','\$5','setex','\$32',zuid,'\$7','3888000',concat('$',length(concat_ws(',',tags))),concat_ws(',',tags)),'\u000d');
EOF

}

function save_app_redis_table()
{
    TABLE_FROM=$1
    TABLE_TO=$2
    REDUCE_NUM=$3

    # change {setex key ttl value }
    #        to redis protocol -->
    #        {*4 \r\n $5 \r\n setex \r\n $length(key) \r\n key \r\n $length(ttl) \r\n ttl \r\n $length(value) \r\n value}
    # example {set b1416e23ff4385668e36639c169c67ca 388800 1100} ==> {*4 \r\n $5 \r\n setex \r\n $32 \r\n b1416e23ff4385668e36639c169c67ca \r\n $6 \r\n 388800 $
    hive <<EOF
    add jar $HIVE_UDF_PATH/zyz_udf2.jar;
    create temporary function crc32  as 'org.apache.hadoop.hive.ql.udf.CRC32_UDF';
    set hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.exec.compress.output=true;
    SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
    set mapreduce.job.reduces=$REDUCE_NUM;
    truncate TABLE $TABLE_TO;
    INSERT overwrite TABLE $TABLE_TO partition(partition_id)
    SELECT concat(concat_ws('\u000d\n','*4','\$5','setex','\$32',zuid,'\$7','3888000',concat('$',length(concat_ws(',',tags))),concat_ws(',',tags)),'\u000d') kv,
           crc32(zuid,'$APP_REDIS_NUM') partition_id
                FROM $TABLE_FROM
                WHERE day_id='$DATE'
                AND length(zuid)=32
                AND size(tags)>0
                group BY crc32(zuid,'$APP_REDIS_NUM'),concat(concat_ws('\u000d\n','*4','\$5','setex','\$32',zuid,'\$7','3888000',concat('$',length(concat_ws(',',tags))),concat_ws(',',tags)),'\u000d');
EOF

}


function save_pc_incr_redis_table()
{
    local EXPIRE_IN_SECONDS=$((30*24*3600))
    local LEN_EXPIRE=`echo -n $EXPIRE_IN_SECONDS|wc -m`

    hive <<EOF
    ADD jar $HIVE_UDF_PATH/zyz_udf2.jar;
    CREATE TEMPORARY FUNCTION crc32 AS 'org.apache.hadoop.hive.ql.udf.CRC32_UDF';
    CREATE TEMPORARY FUNCTION base62 AS 'org.apache.hadoop.hive.ql.udf.Base62';
    SET hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.exec.compress.output=TRUE;
    SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
    SET mapreduce.job.reduces=10;
    SET mapreduce.map.memory.mb=4096;
    SET mapreduce.map.java.opts=-Xmx3686m;
    SET mapreduce.reduce.memory.mb=4096;
    SET mapreduce.reduce.java.opts=-Xmx3686m;
    SET mapreduce.input.fileinputformat.split.maxsize=100000000;
    TRUNCATE TABLE dim_redis_protocol_pc_incr;
    INSERT overwrite TABLE dim_redis_protocol_pc_incr partition(partition_id)
    SELECT concat(concat_ws('\u000d\n','*4','\$5','setex',concat('$',length(uid)),uid,concat('$','$LEN_EXPIRE'),'$EXPIRE_IN_SECONDS',concat('$',length(tags)),tags),'\u000d') kv,
           crc32(uid,"$PC_REDIS_NUM") partition_id
    FROM
      (SELECT uid,
              concat_ws(',',collect_set(base62(cast(tagid AS int)))) tags
       FROM INCR_USER_TAGS_MERGE LATERAL VIEW explode(split(tags,',')) tagTable AS tagid
       WHERE day_id='$DATE'
         AND uid<>''
         AND tags<>''
         AND cast(tagid AS int) > 0
       GROUP BY uid) t
    GROUP BY crc32(uid,"$PC_REDIS_NUM"),
             concat(concat_ws('\u000d\n','*4','\$5','setex',concat('$',length(uid)),uid,concat('$','$LEN_EXPIRE'),'$EXPIRE_IN_SECONDS',concat('$',length(tags)),tags),'\u000d');
EOF
}

function save_pc_whole_redis_table()
{

    local MAX_VALID_DAYS=30

    hive <<EOF
    ADD jar $HIVE_UDF_PATH/zyz_udf2.jar;
    CREATE TEMPORARY FUNCTION crc32 AS 'org.apache.hadoop.hive.ql.udf.CRC32_UDF';
    CREATE TEMPORARY FUNCTION base62 AS 'org.apache.hadoop.hive.ql.udf.Base62';
    SET hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.exec.compress.output=TRUE;
    SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
    SET mapreduce.job.reduces=100;
    SET mapreduce.map.memory.mb=4096;
    SET mapreduce.map.java.opts=-Xmx3686m;
    SET mapreduce.reduce.memory.mb=4096;
    SET mapreduce.reduce.java.opts=-Xmx3686m;
    SET mapreduce.input.fileinputformat.split.maxsize=100000000;
    TRUNCATE TABLE dim_redis_protocol_pc_whole;
    INSERT overwrite TABLE dim_redis_protocol_pc_whole partition(partition_id)
    SELECT concat(concat_ws('\u000d\n','*4','$5','setex',concat('$',length(uid)),uid,concat('$',length((("$MAX_VALID_DAYS"- datediff("$DATE",updtime))*24*3600))),concat('',(("$MAX_VALID_DAYS"- datediff("$DATE",updtime))*24*3600)),concat('$',length(tags)),tags),'\u000d') kv,
           crc32(uid,"$PC_REDIS_NUM") partition_id
    FROM
      (SELECT uid,
              updtime,
              concat_ws(',',collect_set(base62(cast(tagid AS int)))) tags
       FROM WHOLE_USER_TAGS_MERGE LATERAL VIEW explode(split(tags,',')) tagTable AS tagid
       WHERE day_id='$DATE'
         AND updtime>date_sub('$DATE',"$MAX_VALID_DAYS")
         AND uid<>''
         AND tags<>''
         AND cast(tagid AS int) > 0
       GROUP BY uid,
                updtime) t
    GROUP BY crc32(uid,"$PC_REDIS_NUM"),
             concat(concat_ws('\u000d\n','*4','$5','setex',concat('$',length(uid)),uid,concat('$',length(("$MAX_VALID_DAYS"- datediff("$DATE",updtime))*24*3600)),concat('',(("$MAX_VALID_DAYS"- datediff("$DATE",updtime))*24*3600)),concat('$',length(tags)),tags),'\u000d');
EOF
}


function unlzop()
{

    ORIGIN_PATH=$1
    for file in `ls $ORIGIN_PATH`
    do
      for lzo_name in `ls $ORIGIN_PATH"/"${file}`
      do
        lzop -d -U $ORIGIN_PATH/${file}/$lzo_name
      done
    done
    return 0

}

function create_uds_redis_file()
{
    mob_redis_hdfs_path=/opt/zyz/uds/output/redis/mob/$mode
    PC_HDFS_PATH=/user/hive/warehouse/dim_redis_protocol_pc_$mode
    PC_LOCAL_PATH=/opt/zyz/uds/output/redis/pc/$mode
    MOB_HDFS_PATH=/user/hive/warehouse/dim_redis_protocol_mob_$mode
    MOB_LOCAL_PATH=/opt/zyz/uds/output/redis/mob/$mode
    mkdir -p $PC_LOCAL_PATH && mkdir -p $MOB_LOCAL_PATH && mkdir -p $UDS_DATA_PATH

    if [ "$mode" == 'incr' ] ; then
      save_app_redis_table etl_usertags_incr dim_redis_protocol_mob_incr 10
      save_pc_incr_redis_table
    elif [ "$mode" == 'whole' ]; then
      save_whole_app_redis_table usertags_whole_for_redis_backup dim_redis_protocol_mob_whole 100
      save_pc_whole_redis_table
    fi

    #pc
    rm -fr $PC_LOCAL_PATH/*
    hadoop fs -get $PC_HDFS_PATH/* $PC_LOCAL_PATH
    cd ${PC_LOCAL_PATH} &&  tar -zcvf ${UDS_DATA_PATH}/${mode}_${DATE}.tar.gz partition*
    #mob
    rm -fr $MOB_LOCAL_PATH/*
    hadoop fs -get $MOB_HDFS_PATH/* $MOB_LOCAL_PATH
    cd ${MOB_LOCAL_PATH} && tar -zcvf ${APP_DATA_PATH}/${mode}_${DATE}.tar.gz partition*

}

function scp_udsredis_to_redishost()
{
    #### push pc user tag tara to sh05 (shanghai pc master) #####
    log_debug "scp $UDS_DATA_PATH/${mode}_$DATE.tar.gz  sh05:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz"
    cp_files_to_remote_with_retry $UDS_DATA_PATH/${mode}_$DATE.tar.gz  sh05:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz&&
    log_debug "copy to sh05:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz OK"||log_error "copy to sh05:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz error"

    #### push app tag tara to sh05 (shanghai mobi master) #####
    log_debug "scp $APP_DATA_PATH/${mode}_$DATE.tar.gz  sh05:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz"
    cp_files_to_remote_with_retry $APP_DATA_PATH/${mode}_$DATE.tar.gz  sh05:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz&&
    log_debug "copy sh05:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz OK"||log_error "copy sh05:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz error"

    #### push pc user tag tara to bj09 (beijing pc master) #####
    log_debug "scp $UDS_DATA_PATH/${mode}_$DATE.tar.gz  bj09:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz"
    cp_files_to_remote_with_retry $UDS_DATA_PATH/${mode}_$DATE.tar.gz  bj09:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz&&
    log_debug "copy to bj09:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz OK"||log_error "copy to bj09:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz error"

    #### push app tag tara to bj09 (beijing mobi master) #####
    log_debug "scp $APP_DATA_PATH/${mode}_$DATE.tar.gz  bj09:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz"
    cp_files_to_remote_with_retry $APP_DATA_PATH/${mode}_$DATE.tar.gz  bj09:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz&&
    log_debug "copy bj09:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz OK"||log_error "copy bj09:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz error"

    #### push pc tag tara to SC_HOST02 (huanan pc)#####
    log_debug "scp $UDS_DATA_PATH/${mode}_$DATE.tar.gz  SC_HOST02:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz"
    cp_files_to_remote_with_retry $UDS_DATA_PATH/${mode}_$DATE.tar.gz  SC_HOST02:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz&&
    log_debug "copy to SC_HOST02:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz OK"||log_error "copy to SC_HOST02:$REMOTE_PC_DATA_PATH/${mode}_$DATE.tar.gz error"

    #### push app tag tara to SC_HOST02 (nuanan mobi)#####
    log_debug "scp $APP_DATA_PATH/${mode}_$DATE.tar.gz  SC_HOST02:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz"
    cp_files_to_remote_with_retry $APP_DATA_PATH/${mode}_$DATE.tar.gz  SC_HOST02:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz&&
    log_debug "copy SC_HOST02:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz OK"||log_error "copy SC_HOST02:$REMOTE_APP_DATA_PATH/${mode}_$DATE.tar.gz error"
}

function  insert_to_redis()
{
    ### bj09 pc ; bj mobile####
    log_debug "insert bj09 redis begin..."
    ssh bj09 "cd $REMOTE_APP_DATA_PATH/../bin/ && sh ./insert_redis_$mode.sh $DATE $mode &&touch  ${REMOTE_APP_DATA_PATH}/app_${mode}_SUCCESS"&&
    log_debug "insert bj09 redis success..."||log_error "insert bj09 redis error!" &

    ### sh05 pc ; sh05 mobile####
    log_debug "insert sh05 mobi redis begin..."
    ssh sh05 "cd $REMOTE_APP_DATA_PATH/../bin/ && sh ./insert_redis_$mode.sh $DATE $mode &&touch  ${REMOTE_APP_DATA_PATH}/app_${mode}_SUCCESS"&&
    log_debug "insert sh05 redis success..."||log_error "insert sh05 redis error!" &    

    ### sc_host02 mobi ####
    log_debug "insert sc_host02 mobi redis begin..."&&
    ssh sc_host02 "cd $REMOTE_APP_DATA_PATH/../bin/ && sh ./insert_redis_$mode.sh $DATE $mode &&touch  ${REMOTE_APP_DATA_PATH}/app_${mode}_SUCCESS"&&
    log_debug "insert sc_host02 mobi redis success!"||log_error "insert sc_host02 mobi redis error!" &
    wait
}


condition_run_method 0 create_uds_redis_file
condition_run_method $? scp_udsredis_to_redishost
condition_run_method $? insert_to_redis