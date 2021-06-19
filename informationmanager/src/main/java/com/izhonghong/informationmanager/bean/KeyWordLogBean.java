package com.izhonghong.informationmanager.bean;

public class KeyWordLogBean {
    private String keyword;
    private String articleTitle;
    private Long crawlTime;
    private String articleUrl;


    public KeyWordLogBean() {
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
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
