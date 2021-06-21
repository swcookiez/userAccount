package com.izhonghong.crwal.task.user_type;

import com.alibaba.fastjson.JSON;
import com.izhonghong.common.hbase.HBaseGeneral;
import com.izhonghong.common.weibo.SinaWeiboUserTaskUtils;
import com.izhonghong.infomanage.taskissue.util.KafkaProduce;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.izhonghong.common.hbase.HBaseGeneral.getHBaseColValue;


/**
 * @author sw
 * @deploy
 * 添加类型，继承此抽象类即可。
 */
public abstract class CollectType extends TimerTask {

    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static final Logger LOG = Logger.getLogger("crawl-task");
    @Override
    public abstract void run();

    public void sendData(String columnName){
        Date date = new Date();
        LOG.info(" "+columnName+"task begin");
        Producer<String, String> producer = KafkaProduce.getProducer();
        Table table = HBaseGeneral.getTable("crawl:focus_user");
        Scan scan = new Scan();
        scan = HBaseGeneral.addScanColumns(scan,"fn","uid",columnName);
        int i = 0;
        ResultScanner scanner = HBaseGeneral.getScanner(table, scan);
        for (Result result : scanner) {
            String colValue = getHBaseColValue(result,"fn",columnName);
            if (colValue.equals("1")){
                String uid = HBaseGeneral.getHBaseColValue(result, "fn", "uid");
                String uuid = String.valueOf(UUID.randomUUID());
                String task = getSinaJson(uid,uuid);
                producer.send(new ProducerRecord<>("weibo_focus_user_"+format.format(date),task));
                i++;
            }
        }
        LOG.info(" "+columnName+"发送成功数据量: "+i);
        scanner.close();
        producer.close();
        HBaseGeneral.tableClose(table);
    }

    public static String getSinaJson(String uid,String uuid) {
        String url ="http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=100505&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=1&pagebar=1&pl_name=Pl_Official_MyProfileFeed__23&id=100505"
                + uid
                + "&script_uri=/p/100505"
                + uid
                + "/home&feed_type=0&pre_page=0&domain_op=100505";
        SinaWeiboUserTaskUtils.UserExpansionTaskBean uetb = new SinaWeiboUserTaskUtils.UserExpansionTaskBean();
        uetb.setUid(uid);
        uetb.setType(5);
        uetb.setUuid(uuid);
        SinaWeiboUserTaskUtils.ThreeElementBean teb1 = new SinaWeiboUserTaskUtils.ThreeElementBean();
        teb1.setUrl(url);
        List<SinaWeiboUserTaskUtils.ThreeElementBean> list = new ArrayList<SinaWeiboUserTaskUtils.ThreeElementBean>();
        list.add(teb1);
        uetb.setThreeElementBeanList(list);
        return JSON.toJSONString(uetb);
    }
}