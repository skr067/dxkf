package com.kqkj.ct.producer.io;

import com.kqkj.ct.common.bean.DataOut;

import java.io.*;

/**
 * 本地文件数据输出
 */
public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path){
        setPath(path);
    }

    public void setPath(String path){
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Object data) throws Exception {
        write(data.toString());
    }

    /**
     * 将数据字符串生成到文件中
     * @param data
     * @throws Exception
     */
    @Override
    public void write(String data) throws Exception {
        writer.print(data);
        writer.flush();
    }

    /**
     * 释放资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if(writer != null){
            writer.close();
        }
    }
}
