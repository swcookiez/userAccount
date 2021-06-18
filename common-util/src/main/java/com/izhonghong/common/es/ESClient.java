package com.izhonghong.common.es;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

/**
 * ES Cleint class
 */
public class ESClient {

    public static Client client;

    private static final Log log = LogFactory.getLog(ESClient.class);
    /**
     * init ES client
     */
    public static void initEsClient() throws UnknownHostException {
        log.info("初始化es连接开始");

        System.setProperty("es.set.netty.runtime.available.processors", "false");

        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-es-test")//设置ES实例的名称
                .put("client.transport.sniff", false)
                .build();

        client = new PreBuiltTransportClient(esSettings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.2.225"), 9300));
        System.out.println("finish");
        log.info("初始化es连接完成");
    }

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        client.close();
        log.info("es连接关闭");
    }

    public static void main(String[] args) {
        try {
            initEsClient();
            HashMap<String, Object> json = new HashMap<>();
            json.put("name","weibo1");
            json.put("text","微博的蓝瘦香菇文章");
            ElasticSearchBulkOperator.addUpdateBuilderToBulk(ESClient.client
                    .prepareUpdate("weibo_test","type", "1").setDocAsUpsert(true).setDoc(json));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
