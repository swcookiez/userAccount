package com.izhonghong.informationmanager.bean;

public class LogBean {
    private String infoName;
    private String infoSourceName;
    private String articleTitle;
    private Long crawlTime;
    private String articleUrl;

    public LogBean() {
    }

    public LogBean(String infoName, String infoSourceName, String articleTitle, Long crawlTime, String articleUrl) {
        this.infoName = infoName;
        this.infoSourceName = infoSourceName;
        this.articleTitle = articleTitle;
        this.crawlTime = crawlTime;
        this.articleUrl = articleUrl;
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

    public String getArticleTitle() {
        return articleTitle;
    }

    public void setArticleTitle(String articleTitle) {
        this.articleTitle = articleTitle;
    }

    public Long getCrawlTime() {
        return crawlTime;
    }

    public void setCrawlTime(Long crawlTime) {
        this.crawlTime = crawlTime;
    }

    public String getArticleUrl() {
        return articleUrl;
    }

    public void setArticleUrl(String articleUrl) {
        this.articleUrl = articleUrl;
    }
}
