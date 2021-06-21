package com.izhonghong.wordsplitmanager.service;

import com.alibaba.fastjson.JSONArray;
import com.izhonghong.common.mysql.DataSourceUtil;
import com.izhonghong.wordsplitmanager.util.ConfigurationManager;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 2021/4/21 9:47
 *
 * @author sw
 * deploy
 */
@Service
public class WordSplitServiceImp implements WordSplitService{
    static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    static {
        DataSourceUtil.connInit(ConfigurationManager.getProperty("jdbc.url"), ConfigurationManager.getProperty("jdbc.user"),
                ConfigurationManager.getProperty("jdbc.password"));
    }

    @Override
    public void setCustomWord(JSONArray array) {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            String format = formatter.format(new Date());
            conn = DataSourceUtil.getConnection();
            ps = conn.prepareStatement("replace into ik_word_splitter(`word`,`create_date`) values (?,?)");
            for (Object o : array) {
                MySQLExecute.PreInsert(ps,new Object[]{o,format});
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            DataSourceUtil.closeResource(ps,conn);
        }
    }

    @Override
    public String getCustomWord() {
        PreparedStatement ps = null;
        Connection conn = null;
        StringBuilder words = new StringBuilder();
        try {
            conn = DataSourceUtil.getConnection();
            ps = conn.prepareStatement("select `word` from `ik_word_splitter`");
            MySQLExecute.executeQuery(ps, new Object[]{}, new QueryProcess() { //匿名实现
                @Override
                public void process(ResultSet result) throws SQLException {
                    while (result.next()){
                        words.append(result.getString("word")).append("\n");
                    }
                }
            });
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            DataSourceUtil.closeResource(MySQLExecute.getResult(),ps,conn);
        }
        return words.toString();
    }
}


