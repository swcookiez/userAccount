package com.izhonghong.wordsplitmanager.service;

import com.alibaba.fastjson.JSONArray;

/**
 * 2021/4/21 9:47
 *
 * @author sw
 * deploy
 */
public interface WordSplitService {
    //获取mysql自定义分词
    String getCustomWord();
    //设置自定义词
    void setCustomWord(JSONArray array);
}
