-- 分区表
-- 分区表实际上就是对应一个HDFS文件系统上的独立的文件夹，该文件夹下是该分区所有的数据文件。Hive中的分区就是分目录，把一个大的数据集根据业务需要分割成小的数据集。在查询时通过where子句中的表达式选择查询所需要的指定的分区，这样的查询效率会提高很多。

-- 注意：分区字段不能是表中已经存在的数据，可以将分区字段看作表的伪列。
create table dept_partition(
    deptno int,    --部门编号
    dname string, --部门名称
    loc string     --部门位置
)
partitioned by (day string)
row format delimited fields terminated by '\t';
--  load data local inpath '/tmp/zhu.hh/data/dept_20220403.log'
-- into table dept_partition
-- partition(day='20220403');
-- 注意：分区表加载数据时，必须指定分区。
drop table dept_partition;

select * from dept_partition;
describe  formatted  dept_partition;
ALTER TABLE bdc_dws.dws_day_org_pro_inv_ds DROP PARTITION (partition_day = '2018-01-03');
ALTER TABLE dept_partition DROP PARTITION (day <= '20220402');

create table dept_partition_bak
as select * from dept_partition;

create table dept_partition_bak
like  dept_partition;

select * from dept_partition_bak;

insert overwrite table  dept_partition_bak select * from dept_partition;

select
    *
from dept_partition
where day='20220401';
-- 多分区联合查询
select
    *
from dept_partition
where day='20220401'
union
    select
        *
    from dept_partition
    where day='20220402'
union
    select
        *
    from dept_partition
    where day='20220403';

-- 查看分区
 show partitions dept_partition;

-- （1）创建单个分区

alter table dept_partition
add partition(day='20220404');
-- （2）同时创建多个分区（分区之间不能有逗号）

alter table dept_partition
add partition(day='20220405') partition(day='20220406');


-- 删除分区
-- （1）删除单个分区
alter table dept_partition
drop partition (day='20220406');

-- （2）同时删除多个分区（分区之间必须有逗号）
alter table dept_partition
drop partition (day='20220404'), partition(day='20220405');
-- 8）查看分区表结构
desc formatted dept_partition;
-- # Partition Information
-- # col_name              data_type               comment
-- month                   string

-- 分区表二级分区
-- 思考：如何一天的日志数据量也很大，如何再将数据拆分?
-- 1）创建二级分区表

create table dept_partition2(
    deptno int,    -- 部门编号
    dname string, -- 部门名称
    loc string     -- 部门位置
)
partitioned by (day string, hour string)
row format delimited fields terminated by '\t';

-- 2）正常的加载数据
-- （1）加载数据到二级分区表中

load data local inpath '/tmp/zhu.hh/data/dept_20220401.log'
into table dept_partition2
partition(day='20220401', hour='12');
-- （2）查询分区数据

select
    *
from dept_partition2
where day='20220401' and hour='12';
-- 3）把数据直接上传到分区目录上，让分区表和数据产生关联的三种方式
-- （1）方式一：上传数据后修复
-- ①上传数据
dfs -mkdir -p  /user/hive/warehouse/dept_partition2/day=20220401/hour=13
dfs -put /opt/module/hive/datas/dept_20220401.log  /user/hive/warehouse/dept_partition2/day=20220401/hour=13;
-- ②查询数据（查询不到刚上传的数据）

select
    *
from dept_partition2
where day='20220401' and hour='13';
-- ③ todo 执行修复命令
msck repair table dept_partition2;
-- ④再次查询数据

select
    *
from dept_partition2
where day='20220401' and hour='13';
-- （2）方式二：上传数据后添加分区
-- ①上传数据
dfs -mkdir -p  /user/hive/warehouse/dept_partition2/day=20220401/hour=14;
dfs -put /opt/module/hive/datas/dept_20220401.log  /user/hive/warehouse/dept_partition2/day=20220401/hour=14;
-- ②执行添加分区
alter table dept_partition2
add partition(day='20220401',hour='14');
-- ③查询数据
select
    *
from dept_partition2
where day='20220401' and hour='14';
-- 方式三：创建文件夹后load数据到分区
-- ①创建目录
dfs -mkdir -p  /user/hive/warehouse/dept_partition2/day=20220401/hour=15;
-- ②上传数据
load data local inpath '/opt/module/hive/datas/dept_20220401.log'
into table dept_partition2
partition(day='20220401',hour='15');
-- ③查询数据
select
    *
from dept_partition2
where day='20220401' and hour='15';


-----------------------------------------------
-- 动态分区调整
-- 关系型数据库中，对分区表Insert数据时候，数据库自动会根据分区字段的值，将数据插入到相应的分区中，Hive中也提供了类似的机制，即动态分区（Dynamic Partition），只不过，使用Hive的动态分区，需要进行相应的配置。
-- ）开启动态分区参数设置
-- （1）开启动态分区功能（默认true，开启）
set hive.exec.dynamic.partition=true;
-- （2）设置为非严格模式（动态分区的模式，默认strict，表示必须指定至少一个分区为静态分区，nonstrict模式表示允许所有的分区字段都可以使用动态分区）
set  hive.exec.dynamic.partition.mode=nonstrict;
-- （3）在所有执行MR的节点上，最大一共可以创建多少个动态分区。默认1000。
set  hive.exec.max.dynamic.partitions=1000;
-- （4）在每个执行MR的节点上，最大可以创建多少个动态分区。该参数需要根据实际的数据来设定。比如：源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错。
set hive.exec.max.dynamic.partitions.pernode=100;
-- （5）整个MR Job中，最大可以创建多少个HDFS文件。默认100000。
set hive.exec.max.created.files=100000;
-- （6）当有空分区生成时，是否抛出异常。一般不需要设置。默认false。
set hive.error.on.empty.partition=false

create table dept_partition_dynamic(
    id int,
    name string
)
partitioned by (loc int)
row format delimited fields terminated by '\t';
insert into table dept_partition_dynamic
partition(loc)
select
    deptno,
    dname,
    loc
from dept;
insert overwrite table dept_partition_dynamic
partition(loc)
select
    deptno,
    dname,
    loc
from dept;

select  * from dept_partition_dynamic;
show partitions  dept_partition_dynamic;

-- todo 分桶表
-- 分区提供一个隔离数据和优化查询的便利方式。不过，并非所有的数据集都可形成合理的分区。对于一张表或者分区，Hive 可以进一步组织成桶，也就是更为细粒度的数据范围划分。
-- 分桶是将数据集分解成更容易管理的若干部分的另一个技术。
-- 分区针对的是数据的存储路径；分桶针对的是数据文件。

create table stu_buck(
    id int,
    name string
)
clustered by(id)
into 4 buckets
row format delimited fields terminated by '\t';

desc formatted stu_buck;

-- 插入数据的时候自动跑mr 将数据写入不同分桶中
load data local inpath '/tmp/zhu.hh/data/student.txt' into table stu_buck;

select * from stu_buck;

-- 根据结果可知：Hive的分桶采用对分桶字段的值进行哈希，然后除以桶的个数求余的方式决定该条记录存放在哪个桶当中。


---------------------------------------------
--  压缩和存储 （源于hadoop lzo）
-- $ hadoop checknative
-- INFO bzip2.Bzip2Factory: Successfully loaded & initialized native-bzip2 library system-native
-- INFO zlib.ZlibFactory: Successfully loaded & initialized native-zlib library
-- WARN zstd.ZStandardCompressor: Error loading zstandard native libraries: java.lang.InternalError: Cannot load libzstd.so.1 (libzstd.so.1: cannot open shared object file: No such file or directory)!
-- WARN erasurecode.ErasureCodeNative: Loading ISA-L failed: Failed to load libisal.so.2 (libisal.so.2: cannot open shared object file: No such file or directory)
-- WARN erasurecode.ErasureCodeNative: ISA-L support is not available in your platform... using builtin-java codec where applicable
-- INFO nativeio.NativeIO: The native code was built without PMDK support.
-- Native library checking:
-- hadoop:  true /opt/module/hadoop-3.3.4/lib/native/libhadoop.so.1.0.0
-- zlib:    true /lib64/libz.so.1
-- zstd  :  false
-- bzip2:   true /lib64/libbz2.so.1
-- openssl: false EVP_CIPHER_CTX_reset
-- ISA-L:   false Loading ISA-L failed: Failed to load libisal.so.2 (libisal.so.2: cannot open shared object file: No such file or directory)
-- PMDK:    false The native code was built without PMDK support.

-- MR支持的压缩编码
-- 压缩格式	算法	文件扩展名	是否可切分
-- DEFLATE	DEFLATE	.deflate	否
-- Gzip	DEFLATE	.gz	否
-- bzip2	bzip2	.bz2	是
-- LZO	LZO	.lzo	是
-- Snappy	Snappy	.snappy	否
-- 为了支持多种压缩/解压缩算法，Hadoop引入了编码/解码器，如下表所示：
-- Hadoop查看支持压缩的方式hadoop checknative。
-- Hadoop在driver端设置压缩。
-- 压缩格式	对应的编码/解码器
-- DEFLATE	org.apache.hadoop.io.compress.DefaultCodec
-- gzip	org.apache.hadoop.io.compress.GzipCodec
-- bzip2	org.apache.hadoop.io.compress.BZip2Codec
-- LZO	com.hadoop.compression.lzo.LzopCodec
-- Snappy	org.apache.hadoop.io.compress.SnappyCodec
-- 压缩性能的比较：
-- 压缩算法	原始文件大小	压缩文件大小	压缩速度	解压速度
-- gzip	8.3GB	1.8GB	17.5MB/s	58MB/s
-- bzip2	8.3GB	1.1GB	2.4MB/s	9.5MB/s
-- LZO	8.3GB	2.9GB	49.3MB/s	74.6MB/s
-- http://google.github.io/snappy/
-- On a single core of a Core i7 processor in 64-bit mode, Snappy compresses at about 250 MB/sec or more and decompresses at about 500 MB/sec or more.

-- 要在Hadoop中启用压缩，可以配置如下参数（mapred-site.xml文件中）：
-- 参数	默认值	阶段	建议
-- io.compression.codecs
-- （在core-site.xml中配置）	org.apache.hadoop.io.compress.DefaultCodec, org.apache.hadoop.io.compress.GzipCodec, org.apache.hadoop.io.compress.BZip2Codec,
-- org.apache.hadoop.io.compress.Lz4Codec	输入压缩	Hadoop使用文件扩展名判断是否支持某种编解码器
-- mapreduce.map.output.compress	false	mapper输出	这个参数设为true启用压缩
-- mapreduce.map.output.compress.codec	org.apache.hadoop.io.compress.DefaultCodec	mapper输出	使用LZO、LZ4或snappy编解码器在此阶段压缩数据
-- mapreduce.output.fileoutputformat.compress	false	reducer输出	这个参数设为true启用压缩
-- mapreduce.output.fileoutputformat.compress.codec	org.apache.hadoop.io.compress. DefaultCodec	reducer输出	使用标准工具或者编解码器，如gzip和bzip2

-- todo  压缩场景和压缩位置

-- mr 过程中直接读压缩文件
-- 在mr前（io优化） mr中（io优化） mr 后（磁盘优化）
-- 写配置项

-- （1）开启Hive中间传输数据压缩功能（Hive本身也希望自己控制下压缩）
set hive.exec.compress.intermediate=true;
-- （2）开启MapReduce中Map输出压缩功能
set mapreduce.map.output.compress=true;
-- （3）设置MapReduce中Map输出数据的压缩方式
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
-- （4）执行查询语句
select
    count(ename) name
from emp;

select * from emp;

--  开启Reduce输出阶段压缩
-- 当Hive将输出写入到表中时，输出内容同样可以进行压缩。属性hive.exec.compress.output控制着这个功能。用户可能需要保持默认设置文件中的默认值false，这样默认的输出就是非压缩的纯文本文件了。用户可以通过在查询语句或执行脚本中设置这个值为true，来开启输出结果压缩功能。
-- 1）案例实操：
-- （1）开启Hive最终输出数据压缩功能（Hive希望能自己控制压缩）
set hive.exec.compress.output=true;
-- （2）开启MapReduce最终输出数据压缩
set mapreduce.output.fileoutputformat.compress=true;
-- （3）设置MapReduce最终数据输出压缩方式
set mapreduce.output.fileoutputformat.compress.codec =org.apache.hadoop.io.compress.SnappyCodec;
-- （4）测试一下输出结果是否是压缩文件
set mapreduce.job.reduces=3;
-- 查看导出的文件是否是被压缩的
insert overwrite local directory '/opt/module/hive/datas/compress/'
select
    *
from emp
sort by deptno desc;

-- 存储格式  ORC(热门的 最高性能的存储格式) 文件存储格式（和压缩不能化为统一的）
-- 文件存储格式
-- Hive支持的存储数据的格式主要有：TEXTFILE 、SEQUENCEFILE、ORC、PARQUET。
-- 行存储的特点
-- 查询满足条件的一整行数据的时候，列存储则需要去每个聚集的字段找到对应的每个列的值，行存储只需要找到其中一个值，其余的值都在相邻地方，所以此时行存储查询的速度更快。
-- 2）列存储的特点
-- 因为每个字段的数据聚集存储，在查询只需要少数几个字段的时候，能大大减少读取的数据量；每个字段的数据类型一定是相同的，列式存储可以针对性的设计更好的设计压缩算法。
-- TEXTFILE和SEQUENCEFILE的存储格式都是基于行存储的。
-- ORC和PARQUET是基于列式存储的。

create table log_text (
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as textfile;
-- load data local inpath '/tmp/zhu.hh/data/log.data'  into table log_text;
select * from log_text;
select * from log_orc;

insert into table  log_orc_com select * from log_text;
create table log_orc(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress"="NONE"); -- 设置orc存储不使用压缩默认会使用zlib
create table log_orc_com(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc;
dfs -du -h /user/hive/warehouse/log_orc/ ;

insert into table  log_parquet select * from log_text;
create table log_parquet(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as parquet;


select count(1) from log_text; -- 1 row retrieved starting from 1 in 1 s 215 ms (execution: 1 s 181 ms, fetching: 34 ms)
select count(1) from log_orc; --1 row retrieved starting from 1 in 97 ms (execution: 83 ms, fetching: 14 ms)
select count(1) from log_parquet; -- 1 row retrieved starting from 1 in 107 ms (execution: 94 ms, fetching: 13 ms)
select count(1) from log_orc_com; -- 1 row retrieved starting from 1 in 171 ms (execution: 155 ms, fetching: 16 ms) -- 文件很小




-- 压缩和存储组合
-- 0）官网
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC
-- ORC存储方式的压缩：
-- Key	Default	Notes
-- orc.compress	ZLIB	high level compression (one of NONE, ZLIB, SNAPPY)
-- orc.compress.size	262,144	number of bytes in each compression chunk
-- orc.stripe.size	268,435,456	number of bytes in each stripe
-- orc.row.index.stride	10,000	number of rows between index entries (must be >= 1000)
-- orc.create.index	true	whether to create row indexes
-- orc.bloom.filter.columns	""	comma separated list of column names for which bloom filter should be created
-- orc.bloom.filter.fpp	0.05	false positive probability for bloom filter (must >0.0 and <1.0)
-- 注意：所有关于ORCFile的参数都是在HQL语句的TBLPROPERTIES字段里面出现。
-- 1）创建一个ZLIB压缩的ORC存储方式
-- （1）建表语句
-- hive (default)>
-- create table log_orc_zlib(
--     track_time string,
--     url string,
--     session_id string,
--     referer string,
--     ip string,
--     end_user_id string,
--     city_id string
-- )
-- row format delimited fields terminated by '\t'
-- stored as orc
-- tblproperties("orc.compress"="ZLIB");
-- （2）插入数据
-- hive (default)>
-- insert into log_orc_zlib
-- select
--     *
-- from log_text;
-- （3）查看插入后数据
-- hive (default)> dfs -du -h /user/hive/warehouse/log_orc_zlib/ ;
-- 2.78 M  /user/hive/warehouse/log_orc_none/000000_0
-- 2）创建一个SNAPPY压缩的ORC存储方式
-- （1）建表语句
-- hive (default)>
-- create table log_orc_snappy(
--     track_time string,
--     url string,
--     session_id string,
--     referer string,
--     ip string,
--     end_user_id string,
--     city_id string
-- )
-- row format delimited fields terminated by '\t'
-- stored as orc
-- tblproperties("orc.compress"="SNAPPY");
-- （2）插入数据
-- hive (default)>
-- insert into log_orc_snappy
-- select
--     *
-- from log_text;
-- （3）查看插入后数据
-- hive (default)> dfs -du -h /user/hive/warehouse/log_orc_snappy/ ;
-- 3.75 M  /user/hive/warehouse/log_orc_snappy/000000_0
-- ZLIB比Snappy压缩的还小。原因是ZLIB采用的是deflate压缩算法。比snappy压缩的压缩率高。
-- 3）创建一个snappy压缩的parquet存储方式
-- （1）建表语句
-- hive (default)>
-- create table log_parquet_snappy(
--     track_time string,
--     url string,
--     session_id string,
--     referer string,
--     ip string,
--     end_user_id string,
--     city_id string
-- )
-- row format delimited fields terminated by '\t'
-- stored as parquet
-- tblproperties("parquet.compression"="SNAPPY");
-- （2）插入数据
-- hive (default)>
-- insert into log_parquet_snappy
-- select
--     *
-- from log_text;
-- （3）查看插入后数据
-- hive (default)> dfs -du -h /user/hive/warehouse/log_parquet_snappy / ;
-- 6.39 MB  /user/hive/warehouse/ log_parquet_snappy /000000_0
-- 4）存储方式和压缩总结
-- todo 在实际的项目开发当中，Hive表的数据存储格式一般选择：orc或parquet。压缩方式一般选择snappy(速度快，压缩效率不高)、lzo(速度和效率都很高)。
-- orc 加 snappy 是生产环境中常用的 使用ORC+snappy组合！
-- 使用Parquet+LZO组合！

----------------------------------------------------------------------------------------------------------
--  企业级别调优


-- https://help.aliyun.com/document_detail/316594.html
-- EXPLAIN [EXTENDED | DEPENDENCY | AUTHORIZATION] query-sql

explain EXTENDED  select * from log_orc_com limit 2;
--  学会使用 explain 查看执行计划



-- 开启Map端聚合参数设置
-- （1）是否在Map端进行聚合，默认为True
-- set hive.map.aggr = true;
-- （2）在Map端进行聚合操作的条目数目
-- set hive.groupby.mapaggr.checkinterval = 100000;
-- （3）有数据倾斜的时候进行负载均衡（默认是false）
-- set hive.groupby.skewindata = true;
--  即两阶段聚合
-- 当选项设定为true，生成的查询计划会有两个MR Job。
-- 第一个MR Job中，Map的输出结果会随机分布到Reduce中，每个Reduce做部分聚合操作，并输出结果，这样处理的结果是相同的Group By Key有可能被分发到不同的Reduce中，从而达到负载均衡的目的；
-- 第二个MR Job再根据预处理的数据结果按照Group By Key分布到Reduce中（这个过程可以保证相同的Group By Key被分布到同一个Reduce中），最后完成最终的聚合操作（虽然能解决数据倾斜，但是不能让运行速度的更快）。



-- Hive 自 0.14.0 开始，加入了一项 "Cost based Optimizer" 来对HQL执行计划进行优化，这个功能通过"hive.cbo.enable" 来开启。在Hive 1.1.0之后，这个feature是默认开启的，它可以自动优化HQL中多个Join的顺序，并选择合适的Join算法。
-- CBO，成本优化器，代价最小的执行计划就是最好的执行计划。传统的数据库，成本优化器做出最优化的执行计划是依据统计信息来计算的。
-- Hive的成本优化器也一样，Hive在提供最终执行前，优化每个查询的执行逻辑和物理执行计划。这些优化工作是交给底层来完成的。根据查询成本执行进一步的优化，从而产生潜在的不同决策：如何排序连接，执行哪种类型的连接，并行度等等。
-- 要使用基于成本的优化（也称为 CBO），请在查询开始设置以下参数：
-- set hive.cbo.enable=true;
-- set hive.compute.query.using.stats=true;
-- set hive.stats.fetch.column.stats=true;
-- set hive.stats.fetch.partition.stats=true;



-- explain select
--     b.id
-- from bigtable b
-- join (
--     select
--         id
--     from bigtable
--     where id <= 10
-- ) o
-- on b.id = o.id;

-- 使用为此下推， 自动使用的


show tables;

select * from location;

explain
select
    e.ename,
    d.dname,
    l.loc_name
from emp e
join dept d
on d.deptno = e.deptno
join location l
on d.loc = l.loc
where e.empno <7521 ;

explain
select
    e.ename,
    d.dname,
    l.loc_name
from (select * from emp
    where empno < 7521)e
join dept d
on d.deptno = e.deptno
join location l
on d.loc = l.loc;



-- 大表与大表JOIN
-- 使用打散加扩容方式解决数据倾斜问题。
-- 选择其中较大的表做打散处理：
-- hive (default)>
-- select
--     *,
--     concat(id,'-','0 or 1 or 2')
-- from A;t1
-- 选择其中较小的表做扩容处理：
-- hive (default)>
-- select
--     *,
--     concat(id,'-','0')
-- from B
-- union all
--     select
--         *,
--         concat(id,'-','1')
--     from B
-- union all
--     select
--         *,
--         concat(id,'-','2')
--     from B;t2

-- 在map执行前合并小文件，减少map数：CombineHiveInputFormat具有对小文件进行合并的功能（系统默认的格式）。HiveInputFormat没有对小文件合并功能。
-- set hive.input.format= org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- 2）在Map-Reduce的任务结束时合并小文件的设置：
-- 在map-only任务结束时合并小文件，默认true。
-- set hive.merge.mapfiles = true;
-- 在map-reduce任务结束时合并小文件，默认false。
-- set hive.merge.mapredfiles = true;
-- 合并文件的大小，默认256M。
-- set hive.merge.size.per.task = 268435456;
-- 当输出文件的平均大小小于该值时，启动一个独立的map-reduce任务进行文件merge。
-- set hive.merge.smallfiles.avgsize = 16777216;
-- 12.5.1.3  Map端聚合
-- set hive.map.aggr=true;相当于map端执行combiner


-- 调整Reduce个数方法一
-- （1）每个Reduce处理的数据量默认是256MB
-- set hive.exec.reducers.bytes.per.reducer = 256000000
-- （2）每个任务最大的reduce数，默认为1009
-- set hive.exec.reducers.max = 1009
-- （3）计算reducer数的公式
-- N=min(参数2，总输入数据量/参数1)(参数2 指的是上面的1009，参数1值得是256M)
-- 2）调整Reduce个数方法二
-- 在hadoop的mapred-default.xml文件中修改。
-- 设置每个job的Reduce个数。
-- set mapreduce.job.reduces = 15;
-- 3）Reduce个数并不是越多越好
-- （1）过多的启动和初始化Reduce也会消耗时间和资源。
-- （2）另外，有多少个Reduce，就会有多少个输出文件，如果生成了很多个小文件，那么如果这些小文件作为下一个任务的输入，则也会出现小文件过多的问题。
-- 在设置Reduce个数的时候也需要考虑这两个原则：处理大数据量利用合适的Reduce数；使单个Reduce任务处理数据量大小要合适。