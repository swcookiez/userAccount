# 项目模块介绍
## common-util
通用工具module，有如下几种工具类<br>

* json解析工具类，使用的是阿里json完成json解析工作.<br>
* elasticsearch工具类，使用elasticsearch transport获取client效率相较于httpClient较高。还包含读取等操作。
* hbase工具类，主要用来获取连接
* kafka工具类，用来方便获取kafka produce、consumer 
* md5工具类，用于对字符串进行加密处理。
* mysql工具,用于获取mysql连接，内部使用德鲁伊连接池来获取连接。
* weibo工具，产生特定格式采集任务工具。

## hbase-coprocessor
hbase协处理器<br>
* 此模块主要是使用hbase自带的协处理器，完成对HBase构建二级索引的任务

## huizhou-concentric
采集惠州用户发文数据，利用发文数据做用户热度榜单分析<br>
主要业务流程<br>
* 采集账号源接收，持久化到HBase。
* 采集任务定时发送。
* 回采数据接收，使用Spark Streaming消费kafka采集回传数据，实时写入hive(ODS).
* 对写入ODS层数据，进行去重、脱敏、解析等操作写入DWD层并制作成脚本(HQL)。
* 对DWD层数据进行统计、聚合得到结果写入DWS层，时间粒度为1天。
* 对DWS层数据进行再聚合，得到最终结果写入ADS层。
* 使用Spark SQL查询ADS最终结果表获取数据发送至kafka队列，等待消费。
* 使用oozie创建任务流，将制作好的脚本和spark任务连接成任务流，在hue页面上做schedule，每日零晨调度 。

## informationmanager
信息源管理平台，主要实现了采集可视化，不管是采集任务源的添加，采集任务发送0周期，采集回传数据。都可以在页面操作，并得到展示。
主要功能如下<br>
* 采集周期控制，可自定义采集周期，修改后生效。
* 支持账号搜索，可根据name、province、city等信息进行模糊查询，得到账号列表。此功能使用HBase二级索引实现，将需要模糊查询的字段建立索引。
* 信息源添加，可以将自己想要采集的信息源(账号)，添加到账号库中。添加的，账号会被视为重点账号，每日会下发采集任务。
* 信息源日志，被添加的信息源发布的文章，可以在此页面得到展示。
* 信息源采集总量统计
* 关键词添加，被添加的关键词将下发采集任务。
* 关键词日志，根据关键词采集到的日志，可以在此页面得到展示。
* 关键词采集日志统计<br>

本平台主要是采用HBase作为底层数据存储，Spring Boot提供数据接口，Elasticsearch实现二级索引，结合了大数据平台和传统的spring框架实现业务。 

## word-split-manager
elasticsearch自定义分词管理系统<br>
在生产环境中使用elasticsearch，经常会用到elasticsearch分词器，但是elasticsearch自带的中文分词器不是特别友好。于是引入IK分词器，但是IK分词器本身的词库，也不足以支撑生产中复杂的业务。于是我们利用IK分词中可以自定义分词的属性，开发了一个自定分词管理系统，支持热词汇添加，修改。<br>

过程详见：[elasticsearch之自定义IK分词器词库](https://blog.csdn.net/soldiers_A/article/details/116013532?spm=1001.2014.3001.5501)  

## task-hdfs-picture-readwrite
hdfs快照读写，使用接口的形式提供快照(图片)的存储和读取。

## taskIssue
代码介绍
* 采集任务定时调度，结合信息源管理平台
* 采集任务发送kafka
* 采集回传任务接收、去重、入库

## regionalization-taskissue
代码介绍：
* 使用spark做一些离线分析