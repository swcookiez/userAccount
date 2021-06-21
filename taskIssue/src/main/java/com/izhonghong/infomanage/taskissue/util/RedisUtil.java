package com.izhonghong.infomanage.taskissue.util;

import redis.clients.jedis.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RedisUtil {
    static JedisCluster jedisCluster;
    private static String hostAndPorts ="10.248.161.31:7000||10.248.161.31:7001||" +
            "10.248.161.32:7000||10.248.161.32:7001||10.248.161.40:7000||10.248.161.40:7001";
    static JedisPoolConfig jedisPoolConfig ;
    public static JedisCluster getJedisCluster(){
        if (jedisCluster==null){
            int timeOut = 10000;
            Set<HostAndPort> nodes = new HashSet<HostAndPort>();
            jedisPoolConfig  = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大连接数
            jedisPoolConfig.setMaxIdle(20); //最大空闲
            jedisPoolConfig.setMinIdle(20); //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true); //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(5000); //忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(false); //每次获得连接的进行测试
            String[] hosts = hostAndPorts.split("\\|\\|");
            for(String hostport:hosts){
                String[] ipport = hostport.split(":");
                String ip = ipport[0];
                int port = Integer.parseInt(ipport[1]);
                nodes.add(new HostAndPort(ip, port));
            }
            jedisCluster = new JedisCluster(nodes,timeOut, jedisPoolConfig);
        }
        return jedisCluster;
    }

    /**
     *  只有key存在时才能设置过期时间
     *  设置key的过期时间
     */
    public static void expire(String key, int seconds){
        JedisCluster jedis = getJedisCluster();
            jedis.expire(key, seconds);
    }

    /**
     * 批量删除key
     */
    public static void deleteKey(String ...key){
        JedisCluster jedisCluster = getJedisCluster();
        jedisCluster.del(key);
    }

    /**
     * 每月一号删除key
     * @param args 传入需要删除的key
     */
    public static void main(String[] args) {
        JedisCluster jedis = getJedisCluster();
        if (!jedis.get("a").contains("z")){
            System.out.println("a Set不包含z");
            jedis.sadd("a", "b");
        }
        if (jedis.get("a").contains("b")){
            System.out.println("添加b成功");
        }
    }
}
