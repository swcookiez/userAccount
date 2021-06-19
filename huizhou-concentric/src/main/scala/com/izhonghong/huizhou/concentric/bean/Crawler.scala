package com.izhonghong.huizhou.concentric.bean

//采集数据
case class Crawler(
                    deptId:String,
                    deptName:String,
                    id:String,
                    mediaName:String,
                    mediaType:String,
                    crawler_time:Long,
                    created_at:Long,
                    mid:String,
                    name:String,
                    read_count:Int,
                    text:String,
                    title:String,
                    uid:String,
                    url:String,
                    zan_count:Int,
                    followers_count:Int,
                    created_day:String
                  )