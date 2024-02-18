# mysql 相关

# mysql 相关


-- 查询 mysql 数据库中的表的物理大小
SELECT
    table_name AS `Table`,
    round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB`
FROM information_schema.TABLES
WHERE table_schema = 'metastore'
ORDER BY (data_length + index_length) DESC;



-- mysql 中相关时间设置
show tables;



# NOW()：返回当前的日期和时间。例如：
SELECT NOW();
# CURDATE()：返回当前的日期。例如：
SELECT CURDATE();
# CURTIME()：返回当前的时间。例如：
SELECT CURTIME();
# DATE()：提取日期部分。例如：
SELECT DATE(NOW()); -- 2023-09-11
SELECT TIME(NOW());

# YEAR()、MONTH()、DAY()：分别提取年、月、日。例如：
SELECT YEAR(NOW()), MONTH(NOW()), DAY(NOW());

# DATEDIFF()：计算两个日期之间的天数。例如：
SELECT DATEDIFF('2023-09-11', '2023-09-01');
# TIMESTAMPDIFF()：计算两个日期时间之间的差值，可以指定返回的单位（如 MONTH、YEAR、MINUTE 等）。例如：
SELECT TIMESTAMPDIFF(MONTH, '2023-01-01', '2023-09-11');
# DATE_ADD()和DATE_SUB()：添加或减去一个时间间隔。例如：
SELECT DATE_ADD(NOW(), INTERVAL 1 DAY), DATE_SUB(NOW(), INTERVAL 1 DAY);
# STR_TO_DATE()：将字符串转换为日期。例如：

SELECT STR_TO_DATE('11,09,2023', '%d,%m,%Y');



CREATE TABLE `my_table` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `data` VARCHAR(255) NOT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

select current_timestamp;

SELECT @@global.time_zone, @@session.time_zone;

select * from flinkversion.my_table;


-- 修改全局时区
SET GLOBAL time_zone = '+8:00';

-- 修改当前session时区
SET time_zone = '+8:00';

SET time_zone = '0:00';