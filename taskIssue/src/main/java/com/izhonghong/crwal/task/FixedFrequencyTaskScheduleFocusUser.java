package com.izhonghong.crwal.task;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.crwal.task.bean.ScheduleBean;
import com.izhonghong.crwal.task.bean.TypeBean;
import com.izhonghong.crwal.task.user_type.BigVUser;
import com.izhonghong.crwal.task.user_type.CollectType;
import com.izhonghong.crwal.task.user_type.GovernmentUser;
import com.izhonghong.crwal.task.user_type.MediaUser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.util.*;

/**
 * @author sw
 * @deploy
 * 2021/2/25
 * 固定频率采集,BigV、媒体、政务号
 * 允许增加代码块和getType中的collectType类型，其他代码不允许修改。遵循开闭原则
 * 由于前段提供系统不同，额外增加一套读mysql周期时间，写入hBase表代码。
 */
public class FixedFrequencyTaskScheduleFocusUser {
    static Map<String, ScheduleBean> schedule = new HashMap<>();
    static Table table = HBaseGeneral.getTable("crawl:task_period_control");
    static {
        //默认一小时发布一轮,可增加类型
        schedule.put("bigV",new ScheduleBean(3*3600*1000L,new BigVUser(),new Timer()));
        schedule.put("government",new ScheduleBean(3*3600*1000L,new GovernmentUser(),new Timer()));
        schedule.put("media",new ScheduleBean(3*3600*1000L,new MediaUser(),new Timer()));
        init(new String[]{"bigV","government","media"});
    }

    private static void init(String[] types) {
        for (String type : types) {
            ScheduleBean scheduleBean = schedule.get(type);
            Timer timer = scheduleBean.getTimer();
            timer.schedule(scheduleBean.getType(),600*1000L,scheduleBean.getPeriod());
        }
    }

    public static void main(String[] args) {
        TimerTask mainTask = new TimerTask(){
            @Override
            public void run() {
                deal();
            }
        };
        new Timer().schedule(mainTask,0,5*60*1000L);
    }

    static void deal(){
        //查询mysql，得到需要下发的任务类型
        ArrayList<TypeBean> typeBeans = queryTaskInfo();
        for (TypeBean typeBean : typeBeans) {
            //拿到CollectType,和发布频率
            String typeName = typeBean.getTypeName();
            Long periods = typeBean.getPeriods();
            ScheduleBean scheduleBean = schedule.get(typeName);
            if (!periods.equals(scheduleBean.getPeriod())){
                scheduleBean.getTimer().cancel();
                scheduleBean.setTimer(new Timer());
                scheduleBean.setType(getType(typeName));
                if (periods != -1000){ //库中存频率值为-1，代表停止频率采集
                    scheduleBean.getTimer().schedule(scheduleBean.getType(),0L, periods);
                }
                scheduleBean.setPeriod(periods);
            }
        }
    }

    static ArrayList<TypeBean> queryTaskInfo(){
        ArrayList<TypeBean> typeBeans = new ArrayList<TypeBean>();
        Scan scan = new Scan();
        scan = HBaseGeneral.addScanColumns(scan,"fn","typeName","period");
        ResultScanner scanner = HBaseGeneral.getScanner(table, scan);
        for (Result result : scanner) {
            String typeName = HBaseGeneral.getHBaseColValue(result, "fn", "typeName");
            String period = HBaseGeneral.getHBaseColValue(result, "fn", "period");
            Long periodLong = (period.equals(""))?3600L:Long.parseLong(period);
            typeBeans.add(new TypeBean(typeName,periodLong*1000));
        }
        scanner.close();
        return typeBeans;
    }

    //增加类型时，这里也需要增加。
    static CollectType getType(String name){
        switch (name){
            case "bigV":
                return new BigVUser();
            case "government":
                return new GovernmentUser();
            case "media":
                return new MediaUser();
            default:
                return null;
        }
    }
}
