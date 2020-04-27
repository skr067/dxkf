package com.kqkj.ct.common.bean;

import com.kqkj.ct.common.api.Column;
import com.kqkj.ct.common.api.Rowkey;
import com.kqkj.ct.common.api.TableRef;
import com.kqkj.ct.common.constant.Names;
import com.kqkj.ct.common.constant.ValueConstant;
import com.kqkj.ct.common.util.DateUtil;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;


/**
 * 基础的数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();


    protected void start() throws Exception{
        getConnection();
        getAdmin();
    }

    protected void end() throws Exception{
        Admin admin = getAdmin();
        if (admin != null){
            admin.close();
            adminHolder.remove();
        }
        Connection conn = getConnection();
        if (conn != null){
            conn.close();
            connHolder.remove();
        }
    }
    /**
     * 获取连接对象
     */
    protected synchronized Connection getConnection() throws Exception{
        Connection conn = connHolder.get();
        if(conn == null){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }

        return conn;
    }

    /**
     * 获取管理对象
     */
    protected synchronized Admin getAdmin() throws Exception{
        Admin admin = adminHolder.get();
        if(admin == null){
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 创建命名空间，如已存在不需要创建，否则创建新的
     */
    protected void createNamespaceNX(String namespace) throws Exception{
        Admin admin = getAdmin();
        System.out.println(namespace);
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch(NamespaceExistException e){
            e.printStackTrace();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 创建表，如已存在删除创建新的
     * @param name
     * @param familes
     */
    protected void createTableXX(String name,String... familes) throws Exception {

        createTableXX(name,null, null ,familes);
    }
    protected void createTableXX(String name,String coprocessorClass,Integer regionCount,String... familes) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        if(admin.tableExists(tableName)){
            //表存在，删除表
            deleteTable(name);
        }
        //创建表
        createTable(name,coprocessorClass,regionCount,familes);
    }

    private void createTable(String name,String coprocessorClass,Integer regionCount,String... families) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if (families == null || families.length == 0){
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }
        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }
        if(coprocessorClass!=null && !"".equals(coprocessorClass)){
            tableDescriptor.addCoprocessor(coprocessorClass);
        }

        //增加预分区
        if(regionCount == null || regionCount <= 1){
            admin.createTable(tableDescriptor);
        } else {
            //分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    /**
     * 获取查询时startrow,stoprow集合
     */
    protected List<String[]> getStartStorRowkeys(String tel, String start, String end){
        List<String[]> rowkeyss = new ArrayList<>();

        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime,"yyyyMM"));
        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime,"yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()){
            //当前时间
            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");
            int regionNum = genRegionNum(tel, nowTime);

            String startRow = regionNum + "_" +tel + "_" +nowTime;
            String stopRow = startRow + "|";
            String[] rowkeys = {startRow, stopRow};
            rowkeyss.add(rowkeys);
            //月份加1
            startCal.add(Calendar.MONTH,1);
        }
        return rowkeyss;

    }

    /**
     * 计算分区号
     */
    protected int genRegionNum(String tel,String date){

        String usercode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0, 6);
        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //crc效验异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);
        //取模
        int regionNum = crc % ValueConstant.REGION_COUNT;
        return regionNum;
    }


    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    private  byte[][] genSplitKeys(int regionCount){
        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        //0|,1|,2|
        ArrayList<byte[]> bsList = new ArrayList<>();
        for (int i=0;i<splitKeyCount;i++){
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }
        //Collections.sort(bsList,new Bytes.ByteArrayComparator());
        bsList.toArray(bs);
        return bs;
    }

    protected void deleteTable(String name) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 增加数据
     */
    protected void putData(String name, List<Put> puts) throws Exception{
        //获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //增加数据
        table.put(puts);
        //关闭表
        table.close();
    }

    /**
     * 增加对象，自动封装数据，将对象数据直接保存到hbase中
     * @param obj
     * @throws Exception
     */
    protected void putData(Object obj) throws Exception{

        //反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef)clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        Field[] fs = clazz.getDeclaredFields();
        String stringRowkey = "";
        for(Field f : fs){
            Rowkey rowkey = f.getAnnotation(Rowkey.class);
            if(rowkey != null){
                f.setAccessible(true);
                 stringRowkey = (String) f.get(obj);
                 break;
            }
        }

        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowkey));

        for(Field f : fs){
            Column column = f.getAnnotation(Column.class);
            if(column != null){
                String family = column.family();
                String colName = column.column();
                if(colName == null || "".equals(colName)){
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value = (String) f.get(obj);

                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }
        //增加数据
        table.put(put);
        //关闭表
        table.close();

    }

}
