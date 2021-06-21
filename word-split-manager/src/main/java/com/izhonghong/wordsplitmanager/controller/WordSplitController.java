package com.izhonghong.wordsplitmanager.controller;

import com.alibaba.fastjson.JSONArray;
import com.izhonghong.wordsplitmanager.service.WordSplitService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;

/**
 * 2021/4/21 9:36
 *
 * @author sw
 * deploy
 */
@RestController
public class WordSplitController {
    @Autowired
    WordSplitService wordSplitService;

    @RequestMapping(value = "/custom/words/adding",method = RequestMethod.POST)
    public String addCustomWord(@RequestBody JSONArray params){
        wordSplitService.setCustomWord(params);
        return "{\n" +
                "    \"errorCode\": \"0\",\n" +
                "    \"errorMessage\": \"添加成功\",\n" +
                "}";
    }

    @RequestMapping(value="/customWords.html")
    public void getCustomDict(HttpServletRequest request, HttpServletResponse response) {
        try {
            //依据实际业务逻辑，获取词汇组。每个词使用\n分隔。
            String content = wordSplitService.getCustomWord();
            // 返回数据
            OutputStream out = response.getOutputStream();
            // Head需要带上 Last-Modified ETag 属性
            // 此处是输出的文件内容大小,不一定是这个样子,只要保证当文件发生变化时,Last-Modified和ETag也是变化的就OK ,比如也可以是文件的MD5
            response.setHeader("Last-Modified", String.valueOf(content.length()));
            response.setHeader("ETag", String.valueOf(content.length()));
            response.setContentType("text/plain; charset=utf-8");
            out.write(content.getBytes("utf-8"));
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
