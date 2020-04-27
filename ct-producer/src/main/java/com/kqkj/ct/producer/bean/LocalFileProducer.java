package com.kqkj.ct.producer.bean;


import com.kqkj.ct.common.bean.DataIn;
import com.kqkj.ct.common.bean.DataOut;
import com.kqkj.ct.common.bean.Producer;
import com.kqkj.ct.common.util.DateUtil;
import com.kqkj.ct.common.util.NumberUtil;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;

    private volatile  boolean flg =true;
    @Override
    public void setIn(DataIn in) {
        this.in = in;
    }

    @Override
    public void setOut(DataOut out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    @Override
    public void produce() {
        try{
            //读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);

            while(flg){
                //从通讯录中随机查找2个电话号码（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index ;
                while (true) {
                    call2Index = new Random().nextInt(contacts.size());
                    if(call1Index != call2Index){
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机的通话时间
                String startDate = "20190101000000";
                String endDate = "20200101000000";
                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();
                //通话时间
                long calltime = startTime + (long)((endTime - startTime)*Math.random());
                //通话时间字符串
                String callTimeString = DateUtil.format(new Date(calltime), "yyyyMMddHHmmss");
                //生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(3000),4);
                //生成通话记录
                CallLog log = new CallLog(call1.getTel(), call2.getTel(), callTimeString, duration);

                System.out.println(log);
                //讲童话记录刷新到数据文件中
                out.write(log);

                Thread.sleep(500);

            }


        } catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {

        if( in != null){
            in.close();
        }
        if( out != null){
            out.close();
        }

    }
}
