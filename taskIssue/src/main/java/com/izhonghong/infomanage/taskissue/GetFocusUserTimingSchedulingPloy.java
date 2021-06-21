package com.izhonghong.infomanage.taskissue;

import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.infomanage.taskissue.bean.TimingSchedulingBean;
import com.izhonghong.infomanage.taskissue.util.DataSourceUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 2021/3/29 13:25
 * 获取页面配置的定时任务时间段，写入hBase表。后续读取hBase表下发定时任务。
 * @author sw
 * deploy 每日执行一次
 */
public class GetFocusUserTimingSchedulingPloy {
    public static Map<String,String> types = new HashMap();
    static {
        //设定采集账号类型以及rowKey
        types.put("大V库","1001");
        types.put("政务库","1002");
        types.put("媒体库","1003");
    }

    public static void main(String[] args) {
        Table table = HBaseGeneral.getTable("crawl:task_period_control");
        ArrayList<Put> puts = new ArrayList<>();
        ArrayList<TimingSchedulingBean> times = timingSchedulingQuery("select * from t_collect_config");
        for (TimingSchedulingBean time : times) {
            String key = types.get(time.getName());
            if(key!=null){
                String collect_times = time.getCollect_times();
                Put put = new Put(Bytes.toBytes(key));
                if (time.getStatus()==1 && time.getCollect_mode() == 2){
                    put.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("collect_times"),Bytes.toBytes(collect_times));
                }else {
                    put.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("collect_times"),Bytes.toBytes("-1"));
                }
                puts.add(put);
            }
        }
        HBaseGeneral.tableDataWrite(table,puts);
        HBaseGeneral.tableClose(table);
    }

    public static ArrayList<TimingSchedulingBean> timingSchedulingQuery(String sql , Object ... params){
        Connection conn = null ;
        PreparedStatement psmt = null;
        ResultSet resultSet = null ;
        ArrayList<TimingSchedulingBean> timingSchedulingBeans = new ArrayList<TimingSchedulingBean>();
        try {
            conn = DataSourceUtil.getConnection();
            psmt = conn.prepareStatement(sql);
            for(int i=0;i<params.length;i++) {
                psmt.setObject(i+1, params[i]);
            }
            resultSet = psmt.executeQuery();
            while (resultSet.next()){
                String name = resultSet.getString("classify_name");
                String times = resultSet.getString("collect_times");
                int status = resultSet.getInt("status");
                int collect_mode = resultSet.getInt("collect_mode");
                TimingSchedulingBean timingSchedulingBean = new TimingSchedulingBean(name,times,status,collect_mode);
                timingSchedulingBeans.add(timingSchedulingBean);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            DataSourceUtil.closeResource(resultSet,psmt, conn);
        }
        return  timingSchedulingBeans;
    }
}
