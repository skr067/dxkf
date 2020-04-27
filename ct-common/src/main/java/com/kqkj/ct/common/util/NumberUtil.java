package com.kqkj.ct.common.util;

import java.text.DecimalFormat;

/**
 * 数字工具类
 */
public class NumberUtil {
    /**
     * 将数字格式化字符串
     * @param num
     * @param length
     * @return
     */
    public static String format(int num, int length){
        StringBuilder sb = new StringBuilder();
        for (int i=1;i<= length;i++){
            sb.append("0");
        }
        DecimalFormat df = new DecimalFormat(sb.toString());
        return df.format(num);
    }
}
