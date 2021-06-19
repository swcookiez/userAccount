package com.izhonghong.informationmanager.bean;

public class CollectCountBean {
    private String infoName;
    private String infoSourceName;
    private Integer collectLevel;
    private String type;
    private long todayTotal;
    private long monthTotal;
    private String infoState;
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public CollectCountBean() {
    }

    public String getInfoName() {
        return infoName;
    }

    public void setInfoName(String infoName) {
        this.infoName = infoName;
    }

    public String getInfoSourceName() {
        return infoSourceName;
    }

    public void setInfoSourceName(String infoSourceName) {
        this.infoSourceName = infoSourceName;
    }

    public Integer getCollectLevel() {
        return collectLevel;
    }

    public void setCollectLevel(Integer collectLevel) {
        this.collectLevel = collectLevel;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTodayTotal() {
        return todayTotal;
    }

    public void setTodayTotal(long todayTotal) {
        this.todayTotal = todayTotal;
    }

    public long getMonthTotal() {
        return monthTotal;
    }

    public void setMonthTotal(long monthTotal) {
        this.monthTotal = monthTotal;
    }

    public String getInfoState() {
        return infoState;
    }

    public void setInfoState(String infoState) {
        this.infoState = infoState;
    }
}
