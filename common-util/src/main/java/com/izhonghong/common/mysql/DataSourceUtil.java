package com.izhonghong.common.mysql;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;


/**
 * 德鲁伊连接池
 */
public class DataSourceUtil implements Serializable {
    public static DataSource dataSource = null;

    public static void main(String[] args) {
        connInit("jdbc:mysql://114.115.xxx:13306/sw_private?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false",
                "root", "xxx");
        Connection conn = null ;
        PreparedStatement psmt = null;
        ResultSet resultSet = null ;
        try {
            conn = DataSourceUtil.getConnection();
            psmt = conn.prepareStatement("SELECT * FROM zhongda_popular_list WHERE created_day = ?");
            psmt.setObject(1, "2021-06-14");
            resultSet = psmt.executeQuery();
            while (resultSet.next()){
                String uid = resultSet.getString("uid");
                String name = resultSet.getString("name");
                String created_day = resultSet.getString("created_day");
                long article_num = resultSet.getLong("article_num");
                long hot_num = resultSet.getLong("hot_num");
                System.out.println(uid + "\t" + name + "\t" + created_day + "\t" + article_num  + "\t" + hot_num );
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    public static void connInit(String url,String user,String password){ //获取连接时需要先初始化一下。
        try {
            Properties props = new Properties();
            props.setProperty("url", url);
            props.setProperty("username", user);
            props.setProperty("password", password);
            props.setProperty("initialSize", "10"); //初始化大小
            props.setProperty("maxActive", "300"); //最大连接
            props.setProperty("minIdle", "10");  //最小连接
            props.setProperty("maxWait", "60000"); //等待时长
            props.setProperty("timeBetweenEvictionRunsMillis", "2000");//配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            props.setProperty("minEvictableIdleTimeMillis", "600000");//配置连接在连接池中最小生存时间 单位毫秒
            props.setProperty("maxEvictableIdleTimeMillis", "900000"); //配置连接在连接池中最大生存时间 单位毫秒
            props.setProperty("validationQuery", "select 1");
            props.setProperty("testWhileIdle", "true");
            props.setProperty("testOnBorrow", "false");
            props.setProperty("testOnReturn", "false");
            props.setProperty("keepAlive", "true");
            props.setProperty("phyMaxUseCount", "100000");
//            props.setProperty("driverClassName", "com.mysql.jdbc.Driver");
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //提供获取连接的方法
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // 提供关闭资源的方法【connection是归还到连接池】
    // 提供关闭资源的方法 【方法重载】3 dql
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭结果集
        // ctrl+alt+m 将java语句抽取成方法
        closeResultSet(resultSet);
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    public static void closeResource(PreparedStatement preparedStatement,
                                     Connection connection) {
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
