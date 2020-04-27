package com.kqkj.ct.consumer.bean;

import com.kqkj.ct.common.bean.Consumer;
import com.kqkj.ct.common.constant.Names;
import com.kqkj.ct.consumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志的消费者对象
 */
public class CalllogConsumer implements Consumer {

    /**
     * 消费数据
     */
    @Override
    public void consume() {
        try {
            //创建配置对象
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));//类加载器
            //获取flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
            //HBase访问数据对象
            HBaseDao dao = new HBaseDao();
            //初始化
            dao.init();

            //消费数据
            while(true){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    //插入数据
                    //dao.insertData(consumerRecord.value());
                    Calllog log = new Calllog(consumerRecord.value());
                    dao.insertData(log);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
