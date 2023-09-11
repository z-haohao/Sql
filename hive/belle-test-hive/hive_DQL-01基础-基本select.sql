-- hive 查询 官网 https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select

-- SELECT [ALL | DISTINCT] select_expr, select_expr, ...
--   FROM table_reference       -- 从什么表查
--   [WHERE where_condition]   -- 过滤
--   [GROUP BY col_list]        -- 分组查询
--    [HAVING col_list]          -- 分组后过滤
--   [ORDER BY col_list]        -- 排序    统称为hive中4个by
--   [CLUSTER BY col_list
--     | [DISTRIBUTE BY col_list] [SORT BY col_list]
--   ]
--  [LIMIT number]                -- 限制输出的行数

create table if not exists dept(
    deptno int,    -- 部门编号
    dname string,  -- 部门名称
    loc int        -- 部门位置
)
row format delimited fields terminated by '\t';

create table if not exists emp(
    empno int,      -- 员工编号
    ename string,   -- 员工姓名
    job string,     -- 员工岗位（大数据工程师、前端工程师、java工程师）
    sal double,     -- 员工薪资
    deptno int      -- 部门编号
)
row format delimited fields terminated by '\t';



select empno, ename from emp;

select ename AS name,
       deptno   dn
from emp;

select sal + 1
from emp;
select count(*) cnt from emp; -- 当前操作分成两个 map（中间包含group by 和count ）计算 然后reduce （中间包含group by 和count ）算子子再汇总计算

select max(sal) from emp; -- 即上一个步骤中的count 变换为 max

select * from emp limit 2,3; -- 底层不执行 mr 直接抓取数据
select * from emp where sal > 1000; -- where子句中不能使用字段别名。 底层不执行MR程序，先过滤sal > 1000的数据，然后抓取全部数据。  使用WHERE子句，将不满足条件的行过滤掉  2）WHERE子句紧随FROM子句

-- between and  in  is null
select *
from emp
where ename LIKE '小%';
-- % 代表零个或多个字符（任意个字符）。
-- _ 代表一个字符。
-- todo RLIKE子句是Hive中这个功能的一个扩展，其可以通过Java的正则表达式这个更强大的语言来指定匹配条件。 需要整理正则表达式
select *
from emp
where ename RLIKE '^小';

--  and or not
select
    *
from emp
where deptno not IN(30, 20);

-- GROUP BY语句通常会和聚合函数一起使用，按照一个或者多个列队结果进行分组，然后对每个组执行聚合操作 在执行过程中，将在map阶段中，将数据分开计算，group数据后 sum count  在reduce中sum/count
select
    t.deptno,
    avg(t.sal) avg_sal
from emp t
group by t.deptno;

-- 计算emp每个部门中每个岗位的最高薪水。
select deptno,job,max(sal)
from emp
group by deptno,job

-- todo  having与where不同点
-- （1）where后面不能写分组聚合函数，而having后面可以使用分组聚合函数。
-- （2）having只用于group by分组统计语句。

-- 求每个部门的平均薪水大于2000的部门
select e.deptno,
       avg(e.sal) avgSal
from emp e
group by e.deptno
having avgSal > 2000;

-- Hive支持通常的SQL JOIN语句，但是只支持等值连接，（这个版本）支持非等值连接
-- hive 3.1.3
-- spark 3.3.1
-- hadoop 3.3.4
-- 根据员工表和部门表中的部门编号相等，查询员工编号、员工名称和部门名称。
select
     e.empno,
    e.ename,
    d.dname
from emp e
join default.dept d on e.deptno = d.deptno;

-- 使用别名可以简化查询。
-- 区分字段的来源。

-- 内连接：只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来。
select
    e.*,
    d.*
from emp e
join dept d
on e.deptno = d.deptno;

-- 左外连接：JOIN操作符左边表中符合WHERE子句的所有记录将会被返回。
select
    e.empno,
    e.ename,
    d.dname
from emp e
left join dept d
on e.deptno = d.deptno;

-- 右外连接：JOIN操作符右边表中符合WHERE子句的所有记录将会被返回。
select
    e.empno,
    e.ename,
    d.deptno
from emp e
right join dept d
on e.deptno = d.deptno;

-- 满外连接：将会返回所有表中符合WHERE语句条件的所有记录。如果任一表的指定字段没有符合条件的值的话，那么就使用NULL值替代。
select
    e.empno,
    e.ename,
    d.deptno
from emp e
full join dept d
on e.deptno = d.deptno;

create table if not exists location(
    loc int,           -- 部门位置id
    loc_name string   -- 部门位置
)
row format delimited fields terminated by '\t';

describe  formatted location;
select * from location;

select
    e.ename,
    d.dname,
    l.loc_name
from emp e
join dept d
on d.deptno = e.deptno
join location l
on d.loc = l.loc;
-- 大多数情况下，Hive会对每对JOIN连接对象启动一个MapReduce任务。本例中会首先启动一个MapReduce job对表e和表d进行连接操作，然后会再启动一个MapReduce job将第一个MapReduce job的输出和表l进行连接操作。
-- 注意：为什么不是表d和表l先进行连接操作呢？这是因为Hive总是按照从左到右的顺序执行的。


-- 笛卡尔集会在下面条件下产生
-- （1）省略连接条件
-- （2）连接条件无效
-- （3）所有表中的所有行互相连接
select
    empno,
    dname
from emp, dept;


-- todo 排序
-- Order By：全局排序，只有一个Reducer。 在前面map中排序后，在最后一个汇总总在此重排
select * from emp order by sal desc ;
-- 按照别名进行排序
select
    ename,
    sal * 2 twosal
from emp
order by twosal;

-- 多个排序 比如排序时候 按照部门，薪资从高到低 这样部门的排序是有序的，然后再对薪水有序排序
select
    ename,
    deptno,
    sal
from emp
order by deptno, sal;



-- Sort By：对于大规模的数据集order by的效率非常低。在很多情况下，并不需要全局排序，此时可以使用sort by。
-- Sort by为每个reducer产生一个排序文件。每个Reducer内部进行排序，对全局结果集来说不是排序。

set mapreduce.job.reduces; -- 查看默认配置
-- mapreduce.job.reduces 是一个 Hadoop MapReduce 的配置属性，用于指定作业的 reduce 任务数。将该属性设置为 -1 表示禁用 reduce 阶段，即不执行任何 reduce 任务，而是直接输出 map 阶段的结果。
-- 通常情况下，MapReduce 作业的输出需要经过 reduce 阶段进行聚合或排序。如果将 mapreduce.job.reduces 设置为 -1，则不会进行 reduce 阶段的操作，这意味着输出将只包含 map 阶段生成的键值对，而不会进行任何聚合或排序。
-- 当使用 -1 作为 reduce 任务数时，Hadoop 会自动将 map 阶段的输出作为最终输出，而不会再进行排序或聚合。这对于一些特殊的处理场景非常有用，如在 MapReduce 作业中使用分布式缓存或将数据写入外部存储。
-- 需要注意的是，将 mapreduce.job.reduces 设置为 -1 可以加快作业的执行速度，但可能会导致输出结果不符合预期或存在数据倾斜等问题。因此，在使用此配置属性时，需要谨慎考虑应用场景和数据特征，并进行充分的测试和验证。
set mapreduce.job.reduces=3;

-- 每个Reduce内部排序（Sort By） 这个一般不会
select
    *
from emp
sort by deptno desc;
-- 在生成的文件中，数据是有序的
-- # cat sortby-result/
-- 000000_0       .000000_0.crc


-- Distribute By：在有些情况下，我们需要控制某个特定行应该到哪个reducer，通常是为了进行后续的聚集操作。distribute by子句可以做这件事。distribute by类似MR中partition（自定义分区），进行分区，结合sort by使用。
-- 对于distribute by进行测试，一定要分配多reduce进行处理，否则无法看到distribute by的效果。
-- 先按照部门编号分区，再按照员工编号薪资排序。

insert overwrite local directory
'/tmp/zhu.hh/distribute-result'
select
    *
from emp
distribute by deptno
sort by sal desc;
-- 000000_0  000001_0  000002_0
-- # cat 000000_0
-- 7698马八研发2850.030
-- 7782金九\N2450.030
-- 7934小红明讲师1300.030
-- 7654侯七研发1250.030
-- 7900小元讲师950.030
-- 7369张三研发800.030

-- 	distribute by的分区规则是根据分区字段的hash码与reduce的个数进行模除后，余数相同的分到一个区。
-- 	Hive要求distribute by语句要写在sort by语句之前。
-- 	演示完以后mapreduce.job.reduces的值要设置回-1，否则下面分区or分桶表load跑mr的时候有可能会报错。


select
    *
from emp
cluster by deptno;

-- 当distribute by和sort by字段相同时，可以使用cluster by方式。
-- cluster by除了具有distribute by的功能外还兼具sort by的功能。但是排序只能是升序排序，不能指定排序规则为ASC或者DESC。

select
    *
from emp
distribute by deptno
sort by deptno;



select *
from default.business;


show functions like  'date.*';

desc function  extended day;

--