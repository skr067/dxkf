package com.kqkj.ct.common.constant;

import com.kqkj.ct.common.bean.Val;

/**
 * 名称常量枚举值
 */
public enum Names implements Val {
    NAMESPACE("ct")
    ,TABLE("ct:calllog")
    ,CF_CALLER("caller")
    ,CF_CALLEE("callee")
    ,CF_INFO("info")
    ,TOPIC("calllog");

    private String name;

    private Names(String name){
        this.name = name;
    }


    @Override
    public void setValue(Object val) {
        this.name = (String) val;
    }

    @Override
    public String getValue() {
        return name;
    }
}
