if [ -n "$1" ] ;then
    do_date=$1
else
    do_date=`date -d "-1 day" +%Y%m%d`
fi
oneSeconds=86400
sevenSeconds=`expr $oneSeconds \* 7 `
dayTime=`date -d "$do_date" +%s`
weekNum=`date -d "$do_date" +%u`
if [ $weekNum == 1 ]
then
    startTime=$dayTime
else
    startTime=`expr $dayTime - $weekNum \* $oneSeconds + $oneSeconds`
fi
monday=`date -d@$startTime "+%Y%m%d"`
beginMon=`date -d "$do_date" +%Y%m`'01'

echo  '指定日期' $do_date
echo  '所在周一' $monday
echo  '所在月初' $beginMon
sql="
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
insert overwrite table huizhou.ads_concentric_crawl
select deptId,deptName,dayt.id,mediaName,mediaType,
article_number_day,
article_number_week,
article_number_month,
reading_number_day,
reading_number_week,
reading_number_month,
reading_number_ave_day,
reading_number_ave_week,
reading_number_ave_month,
thumb_up_number_day,
thumb_up_number_week,
thumb_up_number_month,
fans_number_day,
'$do_date'
from
(
select * from 
huizhou.dws_concentric_crawl where created_day='$do_date'
)dayt
join
(
select
id,
sum(article_number_day) article_number_week,
sum(reading_number_day) reading_number_week,
sum(reading_number_ave_day) reading_number_ave_week,
sum(thumb_up_number_day) thumb_up_number_week
from huizhou.dws_concentric_crawl
where created_day >= '$monday'
and created_day <= '$do_date'
group by id
)weekt
on dayt.id = weekt.id
join
(
select
id,
sum(article_number_day) article_number_month,
sum(reading_number_day) reading_number_month,
sum(reading_number_ave_day) reading_number_ave_month,
sum(thumb_up_number_day) thumb_up_number_month
from huizhou.dws_concentric_crawl
where created_day >= '$beginMon'
and created_day <= '$do_date'
group by id
)montht
on dayt.id = montht.id;
"
nohup hive -e "$sql" >/home/zhonghong/huizhou-concentric/log/'ads'"$do_date".'log' 2>&1 &
