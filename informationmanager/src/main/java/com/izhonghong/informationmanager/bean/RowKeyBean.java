package com.izhonghong.informationmanager.bean;

public class RowKeyBean {
    private String startKey;
    private String nextKey;
    private String lastKey;

    public String getLastKey() {
        return lastKey;
    }

    public void setLastKey(String lastKey) {
        this.lastKey = lastKey;
    }

    public RowKeyBean() {
    }

    public RowKeyBean(String startKey, String nextKey) {
        this.startKey = startKey;
        this.nextKey = nextKey;
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getNextKey() {
        return nextKey;
    }

    public void setNextKey(String nextKey) {
        this.nextKey = nextKey;
    }
}
