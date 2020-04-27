package com.kqkj.ct.producer;

import com.kqkj.ct.common.bean.Producer;
import com.kqkj.ct.producer.bean.LocalFileProducer;
import com.kqkj.ct.producer.io.LocalFileDataIn;
import com.kqkj.ct.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception{

        if(args.length < 2){
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar path1 path2");
            System.exit(1);
        }
        //构建生产者对象
        Producer producer = new LocalFileProducer();

        //producer.setIn(new LocalFileDataIn("D:\\资料\\大数据\\6.尚硅谷大数据\\30.电信客服综合案例\\2.资料\\辅助文档\\contact.log"));
        //producer.setOut(new LocalFileDataOut("C:\\Users\\Administrator\\Desktop\\asd.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));
        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
