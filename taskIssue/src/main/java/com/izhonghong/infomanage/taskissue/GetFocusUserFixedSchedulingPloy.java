package com.izhonghong.infomanage.taskissue;

import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.infomanage.taskissue.bean.TimingSchedulingBean;
import com.izhonghong.infomanage.taskissue.util.DataSourceUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * 2021/3/30 11:22
 *
 * @author sw
 * deploy
 */
public class GetFocusUserFixedSchedulingPloy {
    public static void main(String[] args) {
        ArrayList<TimingSchedulingBean> times = timingSchedulingQuery("select t2.classify_name,t2.collect_mode,t2.collect_grade,t2.status,t1.interval from t_collection_grade t1,t_collect_config t2\n" +
                "        where t1.grade = t2.collect_grade");
        Table table = HBaseGeneral.getTable("crawl:task_period_control");
        for (TimingSchedulingBean time : times) {
            String key = GetFocusUserTimingSchedulingPloy.types.get(time.getName());
            if (key!=null){
                Put put = new Put(Bytes.toBytes(key));
                if(time.getStatus()==1 && time.getCollect_mode()==1){
                    int interval = time.getInterval();
                    put.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("period"),Bytes.toBytes(String.valueOf(interval*60)));
                }else {
                    put.addColumn(Bytes.toBytes("fn"),Bytes.toBytes("period"),Bytes.toBytes("-1"));
                }
                try {
                    table.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
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
                int times = resultSet.getInt("interval");
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
