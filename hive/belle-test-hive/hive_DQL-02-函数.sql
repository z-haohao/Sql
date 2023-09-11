-- Hive会将常用的逻辑封装成函数给到用户进行使用，类似于java中的api

show functions;
desc function abs; -- 查看函数用法
desc function extended abs;  -- 查看函数详细用法

-- todo 函数分类
-- 单行函数分类: 日期函数，字符串函数，集合函数，数学函数，流程控制函数
/**

 */
-- unix_timestamp:返回当前或指定时间的时间戳 说明：-前面是日期后面是指，日期传进来的具体格式
select unix_timestamp('2022/08/08 08-08-08','yyyy/MM/dd HH-mm-ss'); --1659946088

-- from_unixtime：将时间戳转为日期格式
select from_unixtime(1659946088); -- 2022-08-08 08:08:08

-- current_date：当前日期
select current_date; -- 2022-07-11
-- current_timestamp：当前的日期加时间，并且精确的毫秒
select current_timestamp; -- 2022-07-11 15:32:22.402

-- to_date：抽取日期部分 year：获取年 month：获取月 day：获取日 hour：获取时 minute：获取分 second：获取秒
select to_date('2022-08-08 08:08:08');
select year('2022-08-08 08:08:08');
select month('2022-08-08 08:08:08');
select day('2022-08-08 08:08:08');
select hour('2022-08-08 08:08:08');
select minute('2022-08-08 08:08:08'); -- 8
select second('2022-08-08 08:08:08');  -- 8

-- weekofyear：当前时间是一年中的第几周
select weekofyear('2022-08-08'); -- 32

-- dayofmonth：当前时间是一个月中的第几天
select dayofmonth('2022-08-08');
-- 输出：
-- 8

-- months_between： 两个日期间的月份 说明：前面的-后面的  并且还挺精确
select months_between('2021-08-08','2022-10-09');
-- 输出：
-- -14.03225806


-- add_months：日期加减月
select add_months('2022-08-08',2);
-- 输出：
-- 2022-10-08

-- datediff：两个日期相差的天数 说明：前面-后面
select datediff('2021-08-08','2022-10-09');
-- 输出：
-- -427

-- date_add：日期加天数 date_sub：日期减天数
select date_add('2022-08-08',2);
select date_sub('2022-08-08',2);
-- 2022-08-10
-- 2022-08-06

--last_day：日期的当月的最后一天
select last_day ('2022-08-08');
-- 2022-08-31

-- date_format:将标准日期解析成指定格式字符串
select date_format('2022-08-08','yyyy年-MM月-dd日');
-- 输出：
-- 2022年-08月-08日


-----------------------------------------------------------------------
-- todo 字符串类
-- upper：转大写 lower：转小写 length：长度 trim：前后去空格
select upper('atguigu');
select lower('ATGUIGU');
select length('atguigu');
select trim('   at    guigu    '); --at    guigu

-- lpad：向左补齐，到指定长度   rpad：向右补齐，到指定长度
select lpad('atguigu',10,'*'); -- ***atguigu
select rpad('atguigu',10,'*'); -- atguigu***

-- substring 截取字符串
 desc function extended substring;
select substring("atguigu",2); -- tguigu
select substring("atguigu",-3); -- igu
select substring("atguigu",3,2); --gu

--replace 替换
select replace('atguigu', 'u', 'A'); --atgAigA

-- regexp_replace：支持的正则的替换
select regexp_replace('100-200', '(\\d+)', 'num'); --num-num

-- regexp：字符串能否被正则匹配
select 'dfsaaaa' regexp 'dfsa+';  --true
select 'dfsaaaa' regexp 'dfsb+'; --false

-- regexp_extract 正则匹配字符串分组  即将匹配到的字符按照为止下标取出来
select regexp_extract('1-20-300', '(.*)-(.*)-(.*)', 3); --300

-- repeat 将字符串复制多次
select repeat('123', 3); --123123123

-- split 将字符串进行切割
select split('a-b-c-d','-'); -- ["a","b","c","d"]

-- nvl 替换null值如果为null则替换为后面的值，如果不为null保留值本身
select nvl(null,1); --  1

-- concat 拼接字符串 concat_ws 以指定分隔符拼接字符串或者字符串数组  ！！ CONCAT_WS must be "string or array<string>"
select concat('beijing','-','shanghai','-','shenzhen'); -- beijing-shanghai-shenzhen
select concat_ws('-','beijing','shanghai','shenzhen'); -- beijing-shanghai-shenzhen
select concat_ws('-',array('beijing','shenzhen','shanghai')); -- beijing-shanghai-shenzhen

-- get_json_object
desc function extended get_json_object;
select get_json_object('[{"name":"大海海","sex":"男","age":"25"},{"name":"小宋宋","sex":"男","age":"47"}]','$.[0].name'); -- 大海海
select get_json_object('[{"name":"大海海","sex":"男","age":"25"},{"name":"小宋宋","sex":"男","age":"47"}]','$.[0]');  --{"name":"大海海","sex":"男","age":"25"}


-----------------------------------------------------------------------------------
-- todo 集合函数
-- size： 集合中元素的个数
select size(friends)  from tch; --  每一行数据中的friends集合里的个数   即 friends中的数据 ["bingbing","lili"]

--map：创建map集合 map(key0, value0, key1, value1...)
select map('xiaohai',1,'dahai',2);  -- {"xiaohai":1,"dahai":2}

-- map_keys： 返回map中的key
select map_keys(map('xiaohai',1,'dahai',2)); --  ["xiaohai","dahai"]

-- map_values: 返回map中的value
select map_values(map('xiaohai',1,'dahai',2));  --[1,2]

-- array 声明array集合
select array('1','2','3','4'); -- ["1","2","3","4"]

-- array_contains: 判断array中是否包含某个元素
select array_contains(array('a','b','c','d'),'a');  -- true
-- sort_array： 将array中的元素排序
select sort_array(array('a','d','c')); -- ["a","c","d"]

-- struct 声明struct中的各属性
select struct('name','age','weight'); -- {"col1":"name","col2":"age","col3":"weight"}

-- named_struct 声明struct的属性和值
select named_struct('name','xiaosong','age',18,'weight',80);  -- {"name":"xiaosong","age":18,"weight":80}

-------------------------------------------------------------------------------------------------------------------
-- 数学函数
-- round： 四舍五入
select round(3.3);   -- 3
-- ceil：  向上取整  floor： 向下取整
select ceil(3.1) ;  --  4
select floor(4.8);  -- 4

-----------------------------------------------
-- 流程控制函数
-- 1）case ：多种结果匹配
-- 语法介绍
--     case
--         列/值
--         when  匹配1  then 结果1
--         when  匹配2  then 结果2
--         ....
--         else  结果n
--     end

select case
           '支付宝'
           when '支付宝' then 1
           when '微信' then 2
           else 0
           end;  -- 1


--  if: 条件判断，类似于java中三元运算符
-- 语法介绍 if(条件判断,true,false)
select if(10>5,'正确','错误');  --  正确
select if(10<5,'正确','错误');  --  错误
-----------------------------------------------------------
-- 案例演示
 create  table  employee(
  name string,  --姓名
  sex  string,  --性别
  birthday string, --出生年月
  hiredate string, --入职日期
  job string,   --岗位
  salary double, --薪资
  bonus double,  --奖金
  friends array<string>, --朋友
  children map<string,int> --孩子
);

insert into employee
  values('张无忌','男','1980/02/12','2022/08/09','销售',3000,12000,array('阿朱','小昭'),map('张小无',8,'张小忌',9)),
        ('赵敏','女','1982/05/18','2022/09/10','行政',9000,2000,array('阿三','阿四'),map('赵小敏',8)),
        ('宋青书','男','1981/03/15','2022/04/09','研发',18000,1000,array('王五','赵六'),map('宋小青',7,'宋小书',5)),
        ('周芷若','女','1981/03/17','2022/04/10','研发',18000,1000,array('王五','赵六'),map('宋小青',7,'宋小书',5)),
        ('郭靖','男','1985/03/11','2022/07/19','销售',2000,13000,array('南帝','北丐'),map('郭芙',5,'郭襄',4)),
        ('黄蓉','女','1982/12/13','2022/06/11','行政',12000,null,array('东邪','西毒'),map('郭芙',5,'郭襄',4)),
        ('杨过','男','1988/01/30','2022/08/13','前台',5000,null,array('郭靖','黄蓉'),map('杨小过',2)),
        ('小龙女','女','1985/02/12','2022/09/24','前台',6000,null,array('张三','李四'),map('杨小过',2));
select * from employee;

/**
  4）每个月的入职人数
  5）每个人年龄(年+月)
  6）按照薪资，奖金的和进行倒序排序，如果奖金为null，置位0
  7）每个人有多少个朋友
  8）每个人的孩子的姓名
  9）每个岗位男女各多少人
  10）每个岗位男女各多少人,结果要求如下
 */



------------------------------------------------
-- 聚合函数
-- 多进一出 很多值传入出来一个值
-- 1）普通聚合 count/sum.... 见第6章 6.2.4
-- 2）collect_list 收集并形成list集合，结果不去重

select
  sex,
  collect_list(job)
from
  employee
group by
  sex;

-- 女	["行政","研发","行政","前台"]
-- 男	["销售","研发","销售","前台"]
-- collect_set 收集并形成set集合，结果去重

select
  sex,
  collect_set(job)
from
  employee
group by
  sex;

-- 女	["行政","研发","前台"]
-- 男	["销售","研发","前台"]


/**
  1）每个月的入职人数以及姓名
 */
select
    month(replace(hiredate,'/','-')) month,
    count(1) cn,
    collect_list(name) name_list
from
    employee
group by
    month(replace(hiredate,'/','-'));

--------------------------------------------
-- 炸裂函数
-- 一进多出，出入一个值出来多个值
-- explode 将数组或者map展开(行转列)
select explode(array('a','b','d','c')); -- 这个展示一列

-- json_tuple 取出json字符串中属性的值  (列转行)
select json_tuple('{"name":"王二狗","sex":"男","age":"25"}','name','sex','age');  -- 展示多列


create table movie_info(
    movie string,     --电影名称
    category string   --电影分类
) ;
insert into movie_info
  values('《疑犯追踪》','悬疑,动作,科幻,剧情'),
('《Lie to me》','悬疑,警匪,动作,心理,剧情'),
('《战狼2》'	,'战争,动作,灾难');

select * from movie_info;

-- [42000][10081] Error while compiling statement: FAILED: SemanticException [Error 10081]: UDTF's are not supported outside the SELECT clause, nor nested in expressions 炸裂函数和聚合函数一样不支持和普通列一起查询
select movie,
       explode(split(category,','))
from movie_info;
-- lateral view 侧写
-- 用法：LATERAL VIEW udtf（expression） tableAlias AS columnAlias
-- 解释：lateral view用于和split，explode等UDTF一起使用，它能够将一行数据拆成多行数据，在此基础上可以对拆分后的数据进行聚合。
-- lateral view首先为原始表的每行调用UDTF，UDTF会把一行拆分成一或者多行，lateral view再把结果组合，产生一个支持别名表的虚拟表
select movie,
       category_name
from movie_info
lateral view
explode(split(category,',')) movie_info_tmp as category_name;

-------------------------------------------------------
-- 窗口函数
--窗口函数的概念
-- 窗口函数是高阶函数，分为窗口和函数两个部分，窗口是限定函数的计算范围，函数表示是计算逻辑

-- 1）窗口函数
--   lag(col,n,default_val)：往前第n行数据
--   lead(col,n, default_val)：往后第n行数据
--   first_value (col,true/false)：当前窗口下的第一个值，第二个参数为true，跳过空值
--   last_value (col,true/false)：当前窗口下的最后一个值，第二个参数为true，跳过空值
-- 2）聚合函数
--   max   最大值
--   min   最小值
--   sum   求和
--   avg   平均值
--   count  计数
-- 3）排名分析函数
-- rank       排名相同时会重复总数不会减少
--   dense_rank  排名相同时会重复总数会减少
-- row_number 行号
-- ntile        分组并给上组号

--函数+over([partition by ...] [order by ...] [窗口子句])  over表示开窗
--   over 表示开窗 默认窗口大小会包含所有数据
--   partition by 表示根据字段再划分一个细窗口 相同字段进入同一个细窗口里面 每个窗口之间相互独立 窗口子句对于每个细窗口独立生效
--   order by 表示窗口内按什么排序 如果只有over 表示直接最大窗口排序 如果有partition by 每个细窗口单独排序
--   窗口子句进一步限定范围
--   窗口子句
-- (rows | range) between (unbounded | [num]) preceding and ([num] preceding | current    row | (unbounded | [num]) following
-- (rows | range) between current row and (current row | (unbounded | [num]) following)
-- (rows | range) between [num] following and (unbounded | [num]) following
--       示例：
--       rows between unbounded preceding and unbounded following
--       行的范围为上无边界到下无边界(第一行到最后一行)
--       注：窗口函数是一行一行走的
-- 窗口函数练习

desc extended business;
create table business(
    name string,        -- 顾客
    orderdate string,  -- 下单日期
    cost int             -- 购买金额
)
row format delimited fields terminated by ',';

load data local inpath "/tmp/zhu.hh/data/business.txt"
into table business;

select * from business;
-- 购买过的总人次并保留所有信息
select name,
       orderdate,
       cost,
       count(*) over (rows  between unbounded preceding and unbounded following) cn
from business;

--购买过的累加人次并保留所有信息
select name,
       orderdate,
       cost,
       count(*) over (rows between unbounded preceding and current row ) cn
from business;

-- 购买过的总人数 todo 注：窗口函数的执行次序是在group by 之后
select name,
       count(*) over (rows between unbounded preceding and unbounded following ) cn
from business
group by name;


-- 2022年4月份购买过的顾客及总人数

select name,
       count(*) over (rows between unbounded preceding and unbounded following ) cn
from business
where substr(orderdate,1,7) = '2022-04'
group by name;

select name,
       count(*) over () cn
from business
where substring(orderdate, 1, 7) = '2022-04'
group by name;

--    能发现窗口子句加与不加结果是一致，原因是窗口子句有默认值
-- When ORDER BY is specified with missing WINDOW clause, the WINDOW specification defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
--     当有order by 但是缺少窗口子句时  范围是 上无边界到当前行
-- When both ORDER BY and WINDOW clauses are missing, the WINDOW specification defaults to ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
--      当order by 和 窗口子句都缺少时 范围 上无边界到下无边界

-- 7）查询顾客的购买明细及月购买总额

select name,
       orderdate,
       cost,
        sum(cost) over(partition by month(orderdate))
from business;

-- 8）查询每个顾客的购买明细及购买总额

select name,
       orderdate,
       cost,
        sum(cost) over(partition by name)
from business;

-- 9）查询每个顾客每个月的购买明细及购买总额
select name,
       orderdate,
       cost,
        sum(cost) over(partition by name,month(orderdate))
from business;


-- 10）按照日期将cost累加并保留明细
select name,
       orderdate,
       cost,
        sum(cost) over(order by orderdate)
from business;


-- 11）按照日期将每个顾客cost累加并保留明细
select name,
       orderdate,
       cost,
        sum(cost) over(partition by name order by orderdate)
from business;


-- 12）求出每个顾客上一次和当前一次消费的和并保留明细
select name,
       orderdate,
       cost,
        sum(cost) over(partition by name order by orderdate rows between 1 preceding and current row )
from business;


-- 14）查询每个顾客购买明细以及上次的购买时间和下次购买时间
-- 窗口函数是一行一行走的，走完上一行才走下一行，因此lag函数都是相对应的上一行到当前行
select name,
       orderdate,
       cost,
       lag(orderdate,1,'0000-00-00') over (partition by name order by orderdate),
       lead(orderdate,1,'9999-99-99') over (partition by name order by orderdate)
from business;

-- 注：并不是所有函数都需要写窗口子句
--    rank dense_rank ntile row_number lag lead 这些函数不支持窗口子句
desc function extended lag;



-- 购买过的累加人数
select name,
        count(*) over ( rows between unbounded preceding and  current row )
from business
group by name;



-- 2022年4月份购买过的顾客及总人数 (不是每个人购买几次，即开窗函数，是在group后，得到的数据进行汇总得到的
select name,
        count(*) over ()
from business
where substr(orderdate,1,7) = '2022-04'
group by name;


-- 16）查询前20%时间的订单信息
select *
from (select name,
       orderdate,
       cost,
       ntile(5) over (order by orderdate) n_g
from business)t
where t.n_g = 1;


-- 按照消费金额进行排序
select name,
       orderdate,
       cost,
        rank() over (order by cost) rk,
        dense_rank() over (order by cost) drk,
        row_number() over (order by cost) drk
from business;

-- 注：排名分析函数中不需要写参数，会将排好序数据进行标号
select name,
       orderdate,
       cost,
        rank()       over (partition by name order by cost) rk,
        dense_rank() over (partition by name order by cost) drk,
        row_number() over (partition by name order by cost) drk
from business;

select 'a',
(select count(*) from business  )as cn,
(select count(*) from emp) as cc;


-- select
-- (SELECT COUNT(1)  FROM dc_ods.yg_order_tbl_after_sale_indemnity_ods ) as count ,
-- (SELECT COUNT(1)  FROM dc_ods.yg_order_tbl_after_sale_indemnity_ods_bak20230802  ) as count