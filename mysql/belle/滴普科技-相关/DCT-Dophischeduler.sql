-- 查询 dass 项目中，相关队列关联情况
select
	name 项目名称
	,dev_run_queue 开发环境离线队列
	,dev_realtime_run_queue 开发环境实时队列
	,prd_run_queue 生产环境离线队列
	,prd_realtime_run_queue 生产环境实时队列
from deepexi_daas_security.project
where is_deleted=0
;