package com.izhonghong.infomanage.taskissue;

import com.izhonghong.infomanage.taskissue.util.QueryData;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Timer;
import java.util.TimerTask;


/**
 * @project 信息源管理平台
 * @deploy 10.248.161.31
 * 关键词任务下发
 */
public class KeywordTaskIssue {
    private static final long minute =  60 * 1000L;

    public static void main(String[] args) {
        String sql = "SELECT lexicon,create_time FROM `t_lexicon_detail` WHERE collect_level = ?";
        String collectLevel2TimeSql = "select t_collection_grade.interval from t_collection_grade where grade = ?";
        //信息源管理平台关键词调度
        for (int i = 1; i < 7; i++) {
            int time = QueryData.collectLevel2Time(collectLevel2TimeSql, i);
            KeywordTaskIssueWork taskIssueLev1 = new KeywordTaskIssueWork(sql, i);
            new Timer().schedule(taskIssueLev1, 0,minute*time); //todo 做成脚本每天定时重启才能读到更新的时间频率 且设置delay = minute*time
            System.out.println("grade= "+i+"\ttime= "+time);
        }
        //热搜词任务调度
        taskSchedule("topic",60);
        //前端搜索词任务调度
        taskSchedule("searchWord",15);
        //专题词汇任务调度
        taskSchedule("subjectWords",120);
    }

    private static void taskSchedule(String column,int times){
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (column.equals("subjectWords")) MultiPlatformKeywordTask.subjectWords(column);
                else MultiPlatformKeywordTask.HotSearchWord(column);
            }
        };
        new Timer().schedule(task, 0,times*minute);
    }
}
