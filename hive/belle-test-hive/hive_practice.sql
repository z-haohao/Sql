show tables ;



select  * from stu;


drop table if exists stu;

create table stu(id int, name string, age int, weight double);

insert into stu values
(1,'xiaohaihai',18,75),(2,'xiaosongsong',16,80),
(3,'xiaohuihui',17,60),(4,'xiaoyangyang',16,65);

select * from stu;

-- hive中复杂数据类型 struct  map array
create table tch(
    name string,                    --姓名
    friends array<string>,        --朋友
    students map<string, int>,   --学生
    address struct<street:string,city:string,email:int> --地址
)
row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
lines terminated by '\n';

-- hdfs 中数据
-- songsong,bingbing_lili,xiaohaihai:18_xiaoyangyang:16,hui long guan_beijing_10010
-- yangyang,caicai_susu,xiaosongsong:17_xiaohuihui:16,chao yang_beijing_10011

--  数据上传： hdfs dfs -put tch.text /user/hive/warehouse/tch

select *  from tch;

select friends[1],students['xiaohaihai'],address.street from tch where  name = 'songsong';

-- 数据类型转化
select '1' + 2, cast('1' as int) + 2;

-- DDL
-- CREATE DATABASE [IF NOT EXISTS] database_name
-- [COMMENT database_comment]
-- [LOCATION hdfs_path]
-- [WITH DBPROPERTIES (property_name=property_value, ...)];
create database if not exists db_hive;
create database db_hive2 location '/db_hive2';
show databases like 'db_hive*';

-- 查看库的基本信息
desc database db_hive;

-- 查看库详细信息
desc database extended db_hive;

-- 用户可以使用ALTER DATABASE命令为某个数据库的DBPROPERTIES设置键-值对属性值，来描述这个数据库的属性信息。数据库的其他元数据信息都是不可更改的，包括数据库名和数据库所在的目录位置。
alter database db_hive set dbproperties('createtime'='20220830');


-- 删除库
drop database if exists db_hive;
-- 强制删除库 message:Database db_hive is not empty. One or more tables exist.
drop database if exists db_hive cascade;



-- 表操作
-- CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name
-- [(col_name data_type [COMMENT col_comment], ...)]
-- [COMMENT table_comment]
-- [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
-- [CLUSTERED BY (col_name, col_name, ...)  分桶
-- [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 排序
-- [ROW FORMAT row_format] DELIMITED [FIELDS TERMINATED BY char] [COLLECTION ITEMS TERMINATED BY char]
--              [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char]
--              | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]
--              用户在建表的时候可以自定义SerDe或者使用自带的SerDe。如果没有指定ROW FORMAT或者ROW FORMAT DELIMITED，将会使用自带的SerDe。在建表的时候，用户还需要为表指定列，用户在指定表的列的同时也会指定自定义的SerDe，Hive通过SerDe确定表的具体的列的数据。
--              SerDe是Serialize/Deserilize的简称，Hive使用Serde进行行对象的序列与反序列化。
-- [STORED AS file_format] ：SEQUENCEFILE（二进制序列文件）、TEXTFILE（文本）、RCFILE（列式存储格式文件）。
--                          如果文件数据是纯文本，可以使用STORED AS TEXTFILE。如果数据需要压缩，使用STORED AS SEQUENCEFILE。
-- [LOCATION hdfs_path]
-- [TBLPROPERTIES (property_name=property_value, ...)]
-- [AS select_statement] 后跟查询语句，根据查询结果创建表
-- [LIKE table_name] 允许用户复制现有的表结构，但是不复制数据

-- 默认创建的hive表均为管理表，hive会控制这个表的声明周期，一般不和外部共享

create table if not exists student(
    id int,
    name string
) row format delimited fields terminated by '\t'
location '/user/hive/warehouse/student';

drop table student;

select * from student;

create table if not exists student2 as select id,name from student;
select * from student2;
create table if not exists student3 like student;
select * from student3;

desc formatted student2; -- Table Type:  MANAGED_TABLE       可以查看为管理表，即可以控制自己的声明周期
drop table student2;

-- 管理表和外包表的使用地方： 每天将收集到的网站日志定期流入HDFS文本文件。在外部表（原始日志表）的基础上做大量的统计分析，用到的中间表、结果表使用内部表存储，数据通过SELECT + INSERT进入内部表。

create external table if not exists teacher(
    id int,
    name string
) row format delimited fields terminated by '\t'
location '/school/teacher';


select * from teacher;
desc formatted teacher;  --Table Type: EXTERNAL_TABLE
drop table teacher; -- 删除表后，还会有数据在hadoop中存在

select * from teacher;

-- 管理表和外部表的转换  注意：（'EXTERNAL'='TRUE'）和（'EXTERNAL'='FALSE'）为固定写法，区分大小写！
alter table student3 set tblproperties('EXTERNAL'='TRUE');
alter table student3 set tblproperties('EXTERNAL'='FALSE');
-- 修改表明ALTER TABLE table_name RENAME TO new_table_name

alter table student3 rename to student2;

-- 更新列，列名可以随意修改，列的类型只能小改大，不能大改小（遵循自动转换规则）。
-- ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment]
-- （2）增加和替换列
-- ALTER TABLE table_name ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...)
-- 注：ADD是代表新增一个字段，字段位置在所有列后面（partition列前），REPLACE则是表示替换表中所有字段，REPLACE使用的时候，字段的类型要跟之前的类型对应上，数量可以减少或者增加，其实就是包含了更新列，增加列，删除列的功能。
desc student;

alter table student add columns(age int);
alter table student change column age ages double;
alter table student replace columns(id int, name string); -- 将原来的所有列都替换为 id 和 name 两列，其中 id 的类型为整数，name 的类型为字符串。

-- 删除表
drop table student2;
-- 清空表 Truncate只能删除管理表，不能删除外部表中数据
truncate table student;
create table  if not exists student2 as select  * from student;
select * from student2;
truncate table student2 ; -- 清空表中的数据，但是不会删除表

-- todo DML 数据操作
-- load data [local] inpath '数据的path'  [overwrite] into table student [partition (partcol1=val1,…);
-- load data：表示加载数据。 （2）local：表示从本地加载数据到Hive表；否则从HDFS加载数据到Hive表。（3）inpath：表示加载数据的路径。
-- （4）overwrite：表示覆盖表中已有数据，否则表示追加。（5）into table：表示加载到哪张表。（6）student：表示具体的表。（7）partition：表示上传到指定分区

drop table student;
create table student(
    id int,
    name string
)
row format delimited fields terminated by '\t';

-- 需要在liunx中执行
load data local inpath '/tmp/zhu.hh/student.txt' into table student;
select * from student3;
--  从hdfs中加载数据到表中
load data inpath '/user/atguigu/student.txt'  overwrite into table student;

-- 插入数据
create table student3(
    id int,
    name string
)
row format delimited fields terminated by '\t';
insert into table  student3 values(1,'wangwu'),(2,'zhaoliu');
insert overwrite table student3
select
    id,
    name
from student
where id < 1006;

-- 当前操作用于合并小文件
insert overwrite table student3 select * from student3;
-- insert into：以追加数据的方式插入到表或分区，原有数据不会删除。
-- insert overwrite：会覆盖表中已存在的数据。
-- 注意：insert不支持插入部分字段，并且后边跟select语句时，select之前不能加as，加了as会报错，一定要跟下面的as select区分开。

-- 查询语句中创建表并加载数据
create table if not exists student4
as select id, name from student3;

-- 创建表时，指定文件位置即外部表
create external table if not exists student5(
    id int,
    name string
)
row format delimited fields terminated by '\t'
location '/student4';
-- 将数据入放入到 student4下面
-- Import数据到指定Hive表中 先用export导出后，再将数据导入。并且因为export导出的数据里面包含了元数据，因此import要导入的表不可以存在，否则报错。
import table student2 from '/user/hive/warehouse/export/student';

-- 1）将查询的结果导出到本地

insert overwrite local directory '/opt/module/hive/datas/export/student'
select * from student;
-- 2）将查询的结果格式化导出到本地

insert overwrite local directory '/opt/module/hive/datas/export/student'
row format delimited fields terminated by '\t'
select * from student;
-- 3）将查询的结果导出到HDFS上（没有local）
insert overwrite directory '/user/atguigu/student2'
row format delimited fields terminated by '\t'
select * from student;

-- todo insert导出，导出的目录不用自己提前创建，Hive会帮我们自动创建，但是由于是overwrite，所以导出路径一定要写具体，否则很可能会误删数据。这个步骤很重要，切勿大意。 导入数据如果可以尽量是用 hadoop distcp 这样直接同步文件到另一个集群的目录下

export table default.student to '/user/hive/warehouse/export/student';

