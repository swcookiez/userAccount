package com.izhonghong.informationmanager.bean;

public class KeywordCountBean {
    private String keyword;
    private Long count ;
    private Long increment;


    public KeywordCountBean(String keyword, Long count, Long increment) {
        this.keyword = keyword;
        this.count = count;
        this.increment = increment;
    }

    public Long getIncrement() {
        return increment;
    }

    public void setIncrement(Long increment) {
        this.increment = increment;
    }


    public KeywordCountBean() {
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
