-- 处理埋点相关的数据

linkhouse.dc_stg.sensors_data_profile_topic_stg
linkhouse.dc_stg.sensors_data_event_global_export_data_stg
linkhouse.dc_stg.sensors_data_event_topic_stg

--SET 'pipeline.name' = 'iceberg_sensors_data_event_global_export_data_stg';
-- set the queue that the job submit to
SET 'yarn.application.queue' = 'root.data_collect_rt';
--SET 'yarn.application.name' = 'iceberg_sensors_data_event_global_export_data_stg';
SET 'sql-client.execution.result-mode' = 'tableau';
--创建catalog
CREATE CATALOG hive_catalog WITH (
'type'='iceberg',
'catalog-type'='hive',
'uri'='thrift://10.251.37.6:30470',
'clients'='10',
'property-version'='1',
'warehouse'='hdfs://belleservice/lakehouse/hive');
use catalog hive_catalog;
-- 使用指定库名
USE dc_stg;

CREATE TABLE sensor_data_event_topic_table (
 id STRING,
  msg_value STRING,
  msg_project STRING,
  msg_event STRING,
  app_code STRING,
  app_name STRING,
  distinct_id STRING,
  anonymous_id STRING,
  `type` STRING,
  track_id STRING,
  flush_time STRING,
ods_update_time STRING,
  create_time STRING,
  partition_date INT
)WITH (
    'connector' = 'kafka',
    'topic' = 'sensor_data_event_topic_ods',
    'properties.bootstrap.servers' = 'bjm8-cdh-kafka-prd-10-251-37-43.belle.lan:9092,bjm8-cdh-kafka-prd-10-251-37-44.belle.lan:9092,bjm8-cdh-kafka-prd-10-251-37-45.belle.lan:9092',
    'properties.group.id' = 'test_ty_flink',
    'scan.startup.mode' = 'latest-offset',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'GSSAPI',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/home/usr_data_collect/zhu.hh/code/flink-1.16.1/usr_data_collect.keytab\" principal=\"usr_data_collect\";',
    'format' = 'dct-json'
);


CREATE TABLE sensor_data_print_table_debeziume (
id STRING ,
`type` STRING ,
`event` STRING,
`time` STRING ,
ver STRING ,
app_code STRING ,
app_name STRING ,
_os STRING ,
_os_version STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic_dbz_event_printer',
  'properties.bootstrap.servers' = 'bjm8-bdc-kafka-prd-10-251-35-201.belle.lan:9092,bjm8-bdc-kafka-prd-10-251-35-203.belle.lan:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);

insert into
    sensor_data_print_table_debeziume
SELECT
    *
FROM
    (
        SELECT
            JSON_VALUE(msg_value, '$.distinct_id') AS id,
            JSON_VALUE(msg_value, '$.type') AS `type`,
            JSON_VALUE(msg_value, '$.ver') AS `ver`,
            JSON_VALUE(msg_value, '$.properties.app_code') AS app_code,
            JSON_VALUE(msg_value, '$.properties.app_name') AS app_name,
            JSON_VALUE(msg_value, '$.properties.$os') AS _os,
            JSON_VALUE(msg_value, '$.properties.$os_version') AS _os_version,
            msg_event as `event`,
            create_time as `time`
        FROM
            sensor_data_event_topic_table
        WHERE
            msg_project = 'production'
            AND msg_event IN (
                'printing_click',
                'navigation_menu_usage_detail',
                'mmp_login_login_click'
            )
    ) t1
WHERE
    t1.app_code IN ('retail-mmp', 'MT_POS', 'retail-pos');

yarn.application.name = $ YARN_APP_NAME
SET
    'yarn.application.name' = 'bdc_sensor_event_topic_printer_debeziume';

    (
    'connector' = 'kafka',
    'topic' = 'sensor_data_event_topic',
    'properties.bootstrap.servers' = 'bjm8-cdh-kafka-prd-10-251-37-46.belle.lan:9092,bjm8-cdh-kafka-prd-10-251-37-43.belle.lan:9092,bjm8-cdh-kafka-prd-10-251-37-49.belle.lan:9092',
    'properties.group.id' = 'bdc.dp.sync_to_kafka.sensor_event_topic_printer_debeziume',
    'scan.startup.mode' = 'latest-offset',
    'value.format' = 'json',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'GSSAPI',
    'properties.sasl.kerberos.service.name' = 'kafka',
    'properties.sasl.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/home/usr_data_collect/zhu.hh/usr_data_collect.keytab\" principal=\"usr_data_collect\";'
);

-- 将数据写入topic中