package com.kqkj.ct.consumer;

import com.kqkj.ct.consumer.bean.CalllogConsumer;

/**
 * 启动消费者
 *
 * 使用kafka消费者获取flume采集的数据
 *
 * 将数据存储到hbase中去
 */
public class Bootstrap {

    public static void main(String[] args) throws Exception {
        //创建消费者
        CalllogConsumer consumer = new CalllogConsumer();
        //消费数据
        consumer.consume();

        //关闭资源
        consumer.close();




    }
}
