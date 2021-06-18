package com.izhonghong.common.weibo;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

public class SinaWeiboUserTaskUtils {


    public static void main(String[] args) {
        String start;
        String end;
        int count=10;
        //		while(true){
        for (int i = 0; i < 65535; i += count) {
//            start = Integer.toHexString(i);
//            start = get4(start);
//            end = Integer.toHexString(i + count);
//            end = get4(end);
            System.out.println(i);
        }

        System.out.println(1024 * 1024 * 1024);
        //两个topic 发送任务
        String uid = "7340647609";
        String url = "http://weibo.com/u/"+uid+"?is_all=1&is_search=0&visible=0&is_tag=0&profile_ftype=1&ajaxpagelet=1&page=1";

//        System.out.println(getSinaJson(uid));
    }
    public static String getSinaJson(String uid,String uuid) {
        String url ="http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=100505&is_search=0&visible=0&is_all=1&is_tag=0&profile_ftype=1&page=1&pagebar=1&pl_name=Pl_Official_MyProfileFeed__23&id=100505"
                + uid
                + "&script_uri=/p/100505"
                + uid
                + "/home&feed_type=0&pre_page=0&domain_op=100505";
        UserExpansionTaskBean uetb = new UserExpansionTaskBean();
        uetb.setUid(uid);
        uetb.setType(5);
        uetb.setUuid(uuid);
        ThreeElementBean teb1 = new ThreeElementBean();
        teb1.setUrl(url);
        List<ThreeElementBean> list = new ArrayList<ThreeElementBean>();
        list.add(teb1);
        uetb.setThreeElementBeanList(list);
        return JSON.toJSONString(uetb);
    }


    public static class ThreeElementBean {

        private String parseResultBean;
        private String url;
        private String html;
        private String uid;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getParseResultBean() {
            return parseResultBean;
        }

        public void setParseResultBean(String parseResultBean) {
            this.parseResultBean = parseResultBean;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getHtml() {
            return html;
        }

        public void setHtml(String html) {
            this.html = html;
        }

    }

    public static class UserExpansionTaskBean implements Cloneable {

//    private static final long serialVersionUID = 1434939868746755925L;

        private String uuid;

        private String uid ;

        private String Id;
        /**
         * 区域编码
         */
        private String adCode;
        /**
         * url，html，parseResultBean 三元素集合beanList
         */
        private List<ThreeElementBean> threeElementBeanList;
        /**
         * 任务执行时间，即：加入到
         */
        private long workTime;
        /**
         * 队列类型：0为不插队，1为插队
         */
        private int queueType;
        /**
         * 是否分页：0为否，1为是
         */
        private int pagination;
        /**
         * 任务类型:1为关键词，2为微博ID，3为用户ID
         */
        private int type;
        /**
         * 任务对应的关键词、微博ID或者用户ID
         */
        private String key;
        /**
         * 是否下载评论：0为否，1为是
         */
        private int downloadComments;
        /**
         * 是否下载转发：0为否，1为是
         */
        private int downloadSends;
        /**
         * 是否已经采集过历史数据：0为否，1为是
         */
        private int paged;
        /**
         * 是否包含地区：0表示没有，1表示有
         */
        private int hasArea;

        /**
         * 定制任务类型：1中大200万，2-53万任务
         */
        private String customTask;

        private String startTime;
        private String endTime;

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getEndTime() {
            return endTime;
        }

        public void setEndTime(String endTime) {
            this.endTime = endTime;
        }

        public String getCustomTask() {
            return customTask;
        }

        public void setCustomTask(String customTask) {
            this.customTask = customTask;
        }

        public String getAdCode() {
            return adCode;
        }

        public void setAdCode(String adCode) {
            this.adCode = adCode;
        }

        public String getId() {
            return Id;
        }

        public void setId(String id) {
            Id = id;
        }

        public List<ThreeElementBean> getThreeElementBeanList() {
            return threeElementBeanList;
        }

        public void setThreeElementBeanList(List<ThreeElementBean> threeElementBeanList) {
            this.threeElementBeanList = threeElementBeanList;
        }

        public long getWorkTime() {
            return workTime;
        }

        public void setWorkTime(long workTime) {
            this.workTime = workTime;
        }

        public int getQueueType() {
            return queueType;
        }

        public void setQueueType(int queueType) {
            this.queueType = queueType;
        }

        public int getPagination() {
            return pagination;
        }

        public void setPagination(int pagination) {
            this.pagination = pagination;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public int getDownloadComments() {
            return downloadComments;
        }

        public void setDownloadComments(int downloadComments) {
            this.downloadComments = downloadComments;
        }

        public int getDownloadSends() {
            return downloadSends;
        }

        public void setDownloadSends(int downloadSends) {
            this.downloadSends = downloadSends;
        }

        public int getPaged() {
            return paged;
        }

        public void setPaged(int paged) {
            this.paged = paged;
        }

        public int getHasArea() {
            return hasArea;
        }

        public void setHasArea(int hasArea) {
            this.hasArea = hasArea;
        }

        @Override
        public String toString() {
            return "DpcTaskBean{" +
                    "Id='" + Id + '\'' +
                    ", type=" + type +
                    ", threeElementBeanList=" + threeElementBeanList +
                    ", workTime=" + workTime +
                    ", queueType=" + queueType +
                    ", pagination=" + pagination +
                    ", key='" + key + '\'' +
                    ", downloadComments=" + downloadComments +
                    ", downloadSends=" + downloadSends +
                    ", paged=" + paged +
                    ", hasArea=" + hasArea +
                    '}';
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            UserExpansionTaskBean dpcTaskBean = (UserExpansionTaskBean) super.clone();
            dpcTaskBean.setThreeElementBeanList(null);
            return dpcTaskBean;
        }
    }
}
