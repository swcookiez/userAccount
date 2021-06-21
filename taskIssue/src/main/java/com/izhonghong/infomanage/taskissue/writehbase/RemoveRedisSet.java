package com.izhonghong.infomanage.taskissue.writehbase;

import com.izhonghong.infomanage.taskissue.util.DateUtil;
import com.izhonghong.infomanage.taskissue.util.RedisUtil;
import redis.clients.jedis.JedisCluster;

/**
 * @deploy 10.248.161.31下crontab任务
 * @author sw
 * @date 2020/11/30  13:41
 * 删除去重的Set
 */
public class RemoveRedisSet {
    private static final JedisCluster jedis = RedisUtil.getJedisCluster();
    public static void main(String[] args) {
        deleteKey(DateUtil.getLastNDay(3)+"_weiboKeywordLog");//删除三天前的Set
        deleteKey(DateUtil.getLastNDay(3)+"_selfMediaKeywordLog");//删除三天前的Set
    }

    private static Boolean keyExist(String key){
        Boolean exists = jedis.exists(key);
        return exists;
    }

    private static void deleteKey(String key){
        if (keyExist(key)){
            jedis.del(key);
        }
    }
}
