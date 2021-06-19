package com.izhonghong.huizhou.concentric.bean

/**
 * @author sw
 * 2021/1/11  16:28
 * 榜单统计结果
 */
case class RankList(
                     deptId:String,
                     deptName:String,
                     id:String,
                     mediaName:String,
                     mediaType:String,
                     article_number_day:Long,
                     article_number_week:Long,
                     article_number_month:Long,
                     reading_number_day:Long,
                     reading_number_week:Long,
                     reading_number_month:Long,
                     reading_number_ave_day:Long,
                     reading_number_ave_week:Long,
                     reading_number_ave_month:Long,
                     thumb_up_number_day:Long,
                     thumb_up_number_week:Long,
                     thumb_up_number_month:Long,
                     fans_number_day:Long,
                     created_day:String
                   )

















