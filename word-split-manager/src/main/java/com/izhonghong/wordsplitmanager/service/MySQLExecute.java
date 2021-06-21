package com.izhonghong.wordsplitmanager.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 2021/4/21 10:19
 *
 * @author sw
 * deploy
 */
public class MySQLExecute {
    private static ResultSet result ;//共享变量
    private static final Lock commitLock = new ReentrantLock();

    protected static void PreInsert(PreparedStatement ps,Object[] paras ){
        try {
            if (paras!=null && paras.length > 0){
                for (int i = 0; i < paras.length; i++) {
                    ps.setObject(i+1,paras[i]);
                }
            }
            ps.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    //执行查询，不同的查询语句实现不同的处理过程QueryProcess
    protected static void executeQuery(PreparedStatement ps,Object[] paras ,QueryProcess queryProcess){
        commitLock.lock();
        result = null;
        try {
            if (paras!=null && paras.length > 0){
                for (int i = 0; i < paras.length; i++) {
                    ps.setObject(i+1,paras[i]);
                }
            }
            result = ps.executeQuery();
            queryProcess.process(result);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            commitLock.unlock();
        }
    }

    public static ResultSet getResult() {
        return result;
    }
}
interface QueryProcess{
    void process(ResultSet result) throws SQLException;
}