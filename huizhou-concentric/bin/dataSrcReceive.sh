#! /bin/bash
JAVA_HOME=/usr/java/jdk1.8.0_141-cloudera
export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
export PATH=$PATH:/usr/local/git/bin
job_path=/home/zhonghong/huizhou-concentric
case $1 in
"start")
{
 nohup java -cp $job_path/lib/*:$job_path/jars/huizhou-concentric-1.0-SNAPSHOT.jar com.izhonghong.huizhou.concentric.crawl_task.DataSrcReceive  >$job_path/log/dataSrcReceive.log 2>&1 &
};;
"stop")
{
        echo "停止服务" 
ps -ef | grep com.izhonghong.huizhou.concentric.crawl_task.DataSrcReceive | grep -v grep | awk '{print $2}'|xargs kill >/dev/null 2>&1
};;
esac
