# hive2es(A spark project read data from hive then write to elasticsearch)
* 前提：你的平台无法使用elastic-hadoop插件，无法通过外部表的方式整合hive与elasticsearch
* 项目使用java,scala ,spark_hive ,spark elasticseach,Jest相关jar包
* 配置文件可放到统一配置管理zk中，通过更改配置文件的方式解耦
* java 配置文件xxx.properties
* 项目代码没有经过充分测试，因无配套环境，仅供参考思路
```
#hive2es通用配置文件，从hive中查询出结果，同步到es中
#hive字段需要与es字段严格匹配
#\ 为properties连接符
#${date} 为固定传参，会全局替换为参数值

#es集群ip,一个即可
esNodes= xxx.com

#es http端口号
esPort = 900

#索引名称和类型
resource = indexname/type

#索引http URL
resourceUrl = http://xxx.com:9200/indexname/type

#删除索引数据语句，可使用es Dsl或url 
deleteByQuery = ?q=date:${date}

#hive 制定hive中的主键作为_id,null则使用es自动生成id
hiveId = null

#查询hive语句
hiveSql = select *           \         
                ,othercols   \                                  
          FROM db.tablename a \
         WHERE date = ${date} \
```
