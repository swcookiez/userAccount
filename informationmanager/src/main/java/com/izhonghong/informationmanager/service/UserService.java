package com.izhonghong.informationmanager.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;

/**
 * 定义一些方法供controller使用
 */
public interface UserService {
    /**
     * 说明：暂定不用
     * @return 返回用列表
     */
    ArrayList<String> getUsers(String tableName);

    /**
     * 增加用户
     * 修改用户
     */
    Boolean putUser(JSONArray params);

    /**
     * 删除用户
     */
    void deleteUser(JSONObject params);

    /**
     * 微博、头条、微信账号模糊查询
     */
    String userRegexQuery(String name,String sourceType,Long startRow,Long size,String province,String city,String mediaType);

    /**
     * 关键词添加,前端搜索词和专题词
     */
    String addKeyword(JSONObject Json);

    /**
     * 信息源查询，输入表名和id查询账号库所有信息
     */
    String getUserInfo(String tableName,String id);
}









