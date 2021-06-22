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
insert overwrite table huizhou.dws_concentric_crawl PARTITION (created_day='$do_date')
select deptId,deptName,id,mediaName,mediaType,
article_number_day,
reading_number_day,cast(reading_number_day/article_number_day as BIGINT) reading_number_ave_day,
thumb_up_number_day,fans_number_day from
(select deptId,deptName,id,mediaName,mediaType,
count(1) over(partition by id) article_number_day,
sum(read_count) over(partition by id) reading_number_day,
sum(zan_count) over(partition by id) thumb_up_number_day,
followers_count fans_number_day,
Row_Number() Over(partition by id order by created_at desc) rank
from huizhou.dwd_concentric_crawl where created_day='$do_date')t
where t.rank = 1;
"
nohup hive -e "$sql" >/home/zhonghong/huizhou-concentric/log/'dws'"$do_date".'log' 2>&1 &
