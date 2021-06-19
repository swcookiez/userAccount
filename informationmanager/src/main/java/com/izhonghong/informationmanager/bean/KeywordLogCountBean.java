package com.izhonghong.informationmanager.bean;

public class KeywordLogCountBean {
    private String keyword ;
    private Long count ;

    public KeywordLogCountBean(String keyword, Long count) {
        this.keyword = keyword;
        this.count = count;
    }

    public KeywordLogCountBean() {
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
