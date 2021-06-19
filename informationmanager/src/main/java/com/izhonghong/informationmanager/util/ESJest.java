package com.izhonghong.informationmanager.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;


/**
 * 2021/3/18 14:36
 *
 * @author sw
 * deploy
 */
public class ESJest {
    static JestClientFactory factory = null;

    public static JestClient getClient(){
        if (factory==null)build();
        return factory.getObject();
    }

    public static void build(){
        factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://10.248.161.16:9200")//如果连不上使用，添加多主机
                .multiThreaded(true)
                .maxTotalConnection(20)
                .connTimeout(10000).readTimeout(60000).build());
    }
}

