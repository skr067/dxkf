package com.kqkj.ct.web.service;

import com.kqkj.ct.web.bean.Calllog;

import java.util.List;

public interface CalllogService {
    List<Calllog> queryMonthDatas(String tel, String calltime);

}
