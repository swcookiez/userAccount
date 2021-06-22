if [ -n "$1" ] ;then
    do_date=$1
else 
    do_date=`date -d "-1 day" +%Y%m%d`
fi
echo $do_date
sql="
set hive.execution.engine=mr;
set hive.groupby.skewindata=true;
set mapred.job.reuse.jvm.num.tasks=20;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set mapreduce.job.reduces = 30;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.fetch.task.conversion=more;
set parquet.memory.min.chunk.size=100000;
set hive.exec.reducers.bytes.per.reducer=2560000000;
set hive.optimize.sort.dynamic.partition=true;
set parquet.block.size=268435456;
set mapreduce.map.cpu.vcores=2;
set mapreduce.map.reduce.vcores=4;
set mapreduce.map.memory.mb=8192;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table huizhou.dwd_concentric_crawl PARTITION (created_day='$do_date')
select deptId,deptName,id,mediaName,mediaType,crawler_time,created_at,mid,read_count,zan_count,followers_count
from(select * ,Row_Number() Over (Partition By id,mid order by created_at desc)rank From huizhou.ods_concentric_crawl where created_day='$do_date')t1
where t1.rank = 1;
"
nohup hive -e "$sql" >/home/zhonghong/huizhou-concentric/log/'dwd'"$do_date".'log' 2>&1 &
