package com.izhonghong.infomanage.taskissue;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

/**
 * @project 信息源管理平台
 * @deploy 10.248.161.31
 * 用户源任务下发
 * @deprecated 任务已走接口形式下发，弃用
 */
public class InfosrcTaskIssuue {
    private static long minute =  60 * 1000L;
    public static void main(String[] args) {
        InfosrcTaskIssueWork work1 = new InfosrcTaskIssueWork("1");
        InfosrcTaskIssueWork work2 = new InfosrcTaskIssueWork("2");
        InfosrcTaskIssueWork work3 = new InfosrcTaskIssueWork("3");
        InfosrcTaskIssueWork work4 = new InfosrcTaskIssueWork("4");
        InfosrcTaskIssueWork work5 = new InfosrcTaskIssueWork("5");
        InfosrcTaskIssueWork work6 = new InfosrcTaskIssueWork("6");
        new Timer().schedule(work1, 0,minute*15);
        new Timer().schedule(work2, 0,minute*10);
        new Timer().schedule(work3, 0,minute*30);
        new Timer().schedule(work4, 0,minute*60);
        new Timer().schedule(work5, 0,minute*120);
        new Timer().schedule(work6, 0,minute*720);
    }


}
