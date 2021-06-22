#!/bin/sh

#根据当前时间获取本周的周一零点到下周一零点时间
#./getWeek.sh

#根据传入的时间戳获取
#./getWeek.sh -t 12356783

#根据传入的日期获取
#./getWeek.sh -d 2015-12-05
oneSeconds=86400
sevenSeconds=`expr $oneSeconds \* 7 `

if [ $1 ]
then
    if [ $1 = '-t' ]
    then
        if [ $2 ]
        then
            day=`date -d@$2 +%Y-%m-%d`
        else
            day=`date +%Y-%m-%d`
        fi  
    elif [ $1 = '-d' ]
    then
        if [ $2 ]
        then
            day=$2
        else
            day=`date +%Y-%m-%d`
        fi  
    else
            day=`date +%Y-%m-%d`
    fi  
else
    day=`date +%Y-%m-%d`
fi

dayTime=`date -d "$day" +%s`

weekNum=`date -d "$day" +%u`
echo $weekNum
if [ $weekNum == 1 ] 
then
    startTime=$dayTime
else
    startTime=`expr $dayTime - $weekNum \* $oneSeconds + $oneSeconds`
fi

endTime=`expr $startTime + $sevenSeconds - 1`

echo `date -d@$startTime "+%Y-%m-%d %H:%M:%S"`
echo $startTime

echo `date -d@$endTime "+%Y-%m-%d %H:%M:%S"`
echo $endTime
