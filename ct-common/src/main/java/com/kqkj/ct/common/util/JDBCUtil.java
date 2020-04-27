package com.kqkj.ct.common.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://rm-2zee99dd7c36j2j21zo.mysql.rds.aliyuncs.com:3306/ct?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "fxa3063674";
    private static final String MYSQL_PASSWORD = "as3063674";

    public static Connection getConnection(){
        Connection conn = null;
        try{
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
        } catch(Exception e){
            e.printStackTrace();

        }
        return conn;
    }

}
