package com.kqkj.ct.cache;

import com.kqkj.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {
        //读取MySql的数据
        Map<String,Integer> userMap = new HashMap<String,Integer>();
        Map<String,Integer> dataMap = new HashMap<String,Integer>();

        //读取用户，时间数据
        Connection connection = null;
        PreparedStatement pstat = null;
        ResultSet rs = null;
        try {
            connection = JDBCUtil.getConnection();
            String queryUserSql = "select id,tel from ct_user";
            pstat = connection.prepareStatement(queryUserSql);
            rs = pstat.executeQuery();
            while(rs.next()){
                Integer id = rs.getInt(1);
                String tel = rs.getString(2);
                userMap.put(tel,id);
            }
            rs.close();
            String queryDataSql = "select id,year,month,day from ct_data";
            pstat = connection.prepareStatement(queryDataSql);
            rs = pstat.executeQuery();
            while(rs.next()){
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                if(month.length() == 1){
                    month = "0" + month;
                }
                String day = rs.getString(4);
                if(day.length() == 1){
                    day = "0" + day;
                }
                dataMap.put(year+month+day,id);
            }
        } catch(Exception e){

        } finally {
            if(rs!=null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(pstat!=null){
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        //向redis存储数据
        Jedis jedis = new Jedis("kqkj103", 6379);
        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()){
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user",key,""+value);
        }
        keyIterator = dataMap.keySet().iterator();
        while (keyIterator.hasNext()){
            String key = keyIterator.next();
            Integer value = dataMap.get(key);
            jedis.hset("ct_data",key,""+value);
        }

    }
}
