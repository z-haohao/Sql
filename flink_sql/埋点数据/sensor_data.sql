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