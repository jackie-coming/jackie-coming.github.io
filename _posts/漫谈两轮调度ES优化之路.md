## 一、业务背景 
我们团队是两轮智能调度引擎，调度的意思是指用算法+人工干预的能力，通过给运维派发任务把站点的单车投放到另外一个站点，获得全局单车收益的最大化。
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718873091388-6f406f29-9504-4596-a5f4-af233bb3eadd.png#averageHue=%23f5f5f4&clientId=u9c79329e-6b4e-4&from=paste&height=366&id=ue8a05a7a&originHeight=732&originWidth=1884&originalType=binary&ratio=2&rotation=0&showTitle=false&size=200297&status=done&style=none&taskId=u4e1f2678-7d5f-4c34-8793-7653b3aa923&title=&width=942)
站点画像用于支撑调度核心链路，属于调度的底层数据画像。在离线场景都有查站点画像的诉求，同时在线场景对查询的耗时要求更高（50ms以内），低搜索等待时间是强制性的，否则会影响调度司机体验。
## 二、问题拆解![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1719821591105-4277ae7a-9353-4b51-86cb-b1ec69c24228.png#averageHue=%239a9472&clientId=uf38e20aa-fa07-4&from=paste&height=1194&id=ub06fc14a&originHeight=1194&originWidth=2184&originalType=binary&ratio=1&rotation=0&showTitle=false&size=692195&status=done&style=none&taskId=u28193170-cad1-4e38-b041-03b47eff05a&title=&width=2184)
站点画像高cpu水位集群带来的问题可以拆分为写入和查询问题。
首先是写入侧flink相关任务同步延迟的问题。我们这边对于站点上有多少单车聚合指标时效性要求高，同时是模型计算调度任务依赖的核心指标，一旦写入侧的同步任务发生较大的延迟，会极大影响模型产出的任务质量。
接着是查询侧耗时高的问题。我们这边有c端红包车推荐的场景，红包车的意思是用户可以通过系统推荐的闲置率较高的车骑行到缺口较多的站点，获取相应的金额，对于c端用户来说，低耗时是必要的，耗时需要控制在50ms以内。
## 三、一些尝试
2023年，长达一年我们团队都苦于站点画像的高水位cpu导致的秒级别高耗时的困扰，同时针对23年新增的场景，我们做了以下尝试
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718868947622-42f8ccdb-5ae2-4741-b073-8ec995023ebb.png#averageHue=%23faf7f6&clientId=u8306ff22-13ec-4&from=paste&height=413&id=u6a89efe2&originHeight=825&originWidth=1608&originalType=binary&ratio=2&rotation=0&showTitle=false&size=149657&status=done&style=none&taskId=ubaa37b3b-f841-4391-8fc2-47e97c848cf&title=&width=804)

首先是23年新增的红包、众包车，是面向c端的在线场景，当时为了解决经纬度附近召回秒级耗时的问题，建设redis缓存支撑c端用户低耗时召回的诉求。当然这种实现方式存在精度问题，召回的诉求是以用户为中心的圆形，但是redis建设的geohash6是长方形，，存在漏召回点位的问题。
然后是23年新增的供需池场景，当时站点画像cpu水位很危险，我们这边建设hbase承接了点查场景。当然之后使用的过程中，发现hbase不稳定，经常抖动，逐渐放弃了使用hbase。 
## 四、技术挑战
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1719824515431-6bd606e2-6343-4dde-a1fa-9854f807a6a8.png#averageHue=%23f8f8f8&clientId=u12b92ff2-6bb2-4&from=paste&height=1184&id=u8d25a410&originHeight=1184&originWidth=2140&originalType=binary&ratio=1&rotation=0&showTitle=false&size=349701&status=done&style=none&taskId=uce51185e-5eca-45f2-9da8-fc9e5036bd6&title=&width=2140)
折腾快一年，23年每次技术方案挑战都有站点画像的身影，（也算是另外一种对底层数据稳定性敏感度的锻炼）尝试了几种方案，人员也换了好几波，接力棒到了我手里，像一个定时炸弹，永远不知道什么时候会爆炸，一旦爆炸，必然是一个事故。同时迫于24年新增场景和现有场景扩量的诉求，对站点画像优化刻不容缓。
## 五、优化过程
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718698372323-530825c9-7179-45a7-beb8-bffe6e22cc0d.png#averageHue=%23f0f3f7&clientId=ua743a0cd-6469-4&from=paste&height=223&id=u73005115&originHeight=1106&originWidth=1546&originalType=binary&ratio=1&rotation=0&showTitle=false&size=187156&status=done&style=none&taskId=u43fa56d4-7b10-4be8-819f-7d3a1e8287b&title=&width=312)![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718698424694-37214d99-d881-4bd1-b4b0-3af1db82dcb8.png#averageHue=%23f0f3f7&clientId=ua743a0cd-6469-4&from=paste&height=225&id=u5383ad3d&originHeight=1104&originWidth=1556&originalType=binary&ratio=1&rotation=0&showTitle=false&size=184871&status=done&style=none&taskId=u39ac0860-22d5-473c-85a3-53e7fd5e3dc&title=&width=317)
我们发现在站点画像构建初期，为了通用性的查询功能，对所有的字段添加了mapping，初步统计发现有282个复杂类型索引（text、integer、float、date、geo_point等） +67个keyword，同时也有很多不需要检索的字段也加上mapping，猜测这么多冗余索引优化完之后应该有很不错的效果。
接下来是es相关的原理拆解分析，官网对于mapping索引类型的建议，以及我们这边在线上落地的整体流程。
#### 5.1、为什么mapping索引会影响集群cpu水位？![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1719459813266-774fe44c-87b9-4821-9f34-69ec6fb90c99.png#averageHue=%23dc862b&clientId=ufa450868-6a80-4&from=paste&height=545&id=u6ddc4c32&originHeight=1090&originWidth=2188&originalType=binary&ratio=2&rotation=0&showTitle=false&size=584530&status=done&style=none&taskId=u97e16a85-c58c-46c7-b734-d2b5b297292&title=&width=1094)
首先从三个思路拆解mapping索引是怎么影响集群的性能？
##### 1、段创建/合并频繁
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718795751033-692196ef-6738-4c5f-8819-655ec4266c88.png#averageHue=%23f6f6f6&clientId=ua02bb000-0977-4&from=paste&height=245&id=u606fa229&originHeight=490&originWidth=1386&originalType=binary&ratio=2&rotation=0&showTitle=false&size=237437&status=done&style=none&taskId=ucf403732-cff3-41bc-b26d-0bb4e92bbec&title=&width=693)
ES集群包含多个节点，索引由分片组成，每个分片都是1个lucene索引实例，Lucene 索引由一个或多个不可变的索引段组成，每个段都是一个倒排索引，每次refresh都会生成一个新段，包含若干个文档的数据。当刷新生成的段文件也越来越多，这时就需要进行段合并，减少段文件的数量，控制段文件的规模。
每个段都会消耗文件句柄、内存和 CPU 周期。而且，每个搜索请求都必须依次检查每个段。段越多，搜索速度就越慢。
##### 2、不需要检索的字段加了mapping，多了一份索引构建的消耗
通过对使用场景的梳理对mapping索引瘦身，主要有以下几种场景
1、确定不需要全文检索的字段，删除text
2、确定不需要聚合、排序检索的字段，删除keyword
3、确定不需要检索的数值类型字段，删除数值类型
4、其他不需要检索，只在返回值里面存在的字段，删除对应的字段类型
通过对倒排索引构建原理拆解分析
###### 倒排索引原理分析
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718855304999-08b7ce7d-a20c-4b66-87f7-6910d1f4b6a6.png#averageHue=%23f9f3f1&clientId=uc346c845-2e04-4&from=paste&height=344&id=RwUCu&originHeight=373&originWidth=670&originalType=binary&ratio=2&rotation=0&showTitle=false&size=120427&status=done&style=none&taskId=u5cbaf9c2-8760-4722-babf-e5f0d4fba14&title=&width=618)
> "All problems in computer science can be solved by another level of indirection." -David J. Wheeler
> “计算机科学中的所有问题都可以通过增加一个间接层来解决。”– David J. Wheeler

Lucene内最核心的倒排索引，本质上就是Term到所有包含该Term的文档的DocId列表的映射。包含以下数据结构

1. term index：为了解决term dictionary占用太大，不适合放在内存。采用fst（Finite State Transducers）保存term字典的二级索引，用于加速查询，可以在FST上快速定位到term在磁盘上的block位置。fst可以参考[https://www.shenyanchao.cn/blog/2018/12/04/lucene-fst/](https://www.shenyanchao.cn/blog/2018/12/04/lucene-fst/)，主要是通过共享前缀和后缀节省空间。
2. term dictionary：为了解决如何快速的在海量term中查询到对应的term，将term排序，保存了每个term对应的docId的列表。
3. Posting List：采用frame of reference进行压缩和用skipList的结构保存用于快速跳跃，多字段联合检索交/并集的时候会用到。
##### 3、错用的数据类型
看到兵哥写的关于慢查询排查的文章[https://tech.hellobike.com/#/article/3006129730349957139](https://tech.hellobike.com/#/article/3006129730349957139)，受到启发，结论是ES5.x之后对数值型字段做TermQuery会很慢，耗时主要在bitset的生成过程，我们这边也有不少数值型的索引，例如下面这个查询，这个查询非常简单，就是7个过滤条件求交集。
```json
{
    "size": 800,
    "query": {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "city_guid.keyword": [
                            "E8B3DF0D9B4F45F9BDF2384DB630B032"
                        ],
                        "boost": 1
                    }
                },
                {
                    "terms": {
                        "template_type": [
                            6
                        ],
                        "boost": 1
                    }
                },
                {
                    "term": {
                        "is_delete": {
                            "value": 0,
                            "boost": 1
                        }
                    }
                },
                {
                    "terms": {
                        "operate_type": [
                            0,
                            1,
                            2
                        ],
                        "boost": 1
                    }
                },
                {
                    "terms": {
                        "operate_unit": [
                            0,
                            1,
                            2,
                            3,
                            4,
                            5
                        ],
                        "boost": 1
                    }
                },
                {
                    "terms": {
                        "area_status": [
                            5
                        ],
                        "boost": 1
                    }
                },
                {
                    "terms": {
                        "business_type": [
                            2,
                            3
                        ],
                        "boost": 1
                    }
                }
            ],
            "adjust_pure_negative": true,
            "boost": 1
        }
    }
}
```
这几个数值型的字段不用于范围查找，字段类型设置上keyword比number更合理。
###### 原理分析
Lucene从6.0开始引入了Block k-d(K-Dimensional) tree来重新设计数值类型的索引结构，其目标是让数值型数据索引的结构更紧凑，搜索速度更快。这种数据结构是为多维数值字段设计的，可以高效的用于诸如地理位置这类数据的快速过滤，但同样适用于单维度的数值型。
对于单维度的数据，实际上就是简单的对所有值做一个排序，然后反复从中间做切分，生成一个类似于B-tree这样的结构。和传统的B-tree不同的是，他的叶子结点存储的不是单值，而是一组值的集合，也就是一个Block。每个Block内部包含的值数量控制在512- 1024个，保证值的数量在block之间尽量均匀分布。 其数据结构大致看起来是这样的:

![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718768646116-c5eda068-4642-40a0-acc7-7570d92be901.png#averageHue=%23e8e8e8&clientId=u6b12bf76-ae79-4&from=paste&height=352&id=XTtOo&originHeight=419&originWidth=542&originalType=binary&ratio=2&rotation=0&showTitle=false&size=88076&status=done&style=none&taskId=u02129a47-afa3-4ef4-b1a3-6d6a75358df&title=&width=455)
Lucene将这颗B-tree的非叶子结点部分放在内存里，而叶子结点紧紧相邻存放在磁盘上。当作range查询的时候，内存里的B-tree可以帮助快速定位到满足查询条件的叶子结点块在磁盘上的位置，之后对叶子结点块的读取几乎都是顺序的。
数值型字段的TermQuery被转换为了PointRangeQuery。这个Query利用Block k-d tree进行范围查找速度非常快，但是满足查询条件的docid集合在磁盘上并非向Postlings list那样按照docid顺序存放，也就无法实现postings list上借助跳表做蛙跳的操作。 要实现对docid集合的快速advance操作，只能将docid集合拿出来，做一些操作再处理。 主要逻辑就是构造成一个代表docid集合的bitset，这个过程和构造Query cache的过程非常类似。 之后advance操作，就是在这个bitset上完成的，构建巨大的bitset是非常消耗时间的。
### 5.2、正确的mapping类型怎么选择？

![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718704370668-3262a312-ee55-461d-9e1c-5f6acfb8cdf3.png#averageHue=%23e7e4e9&clientId=u6b12bf76-ae79-4&from=paste&height=613&id=ufa3b05c8&originHeight=1226&originWidth=2408&originalType=binary&ratio=2&rotation=0&showTitle=false&size=435498&status=done&style=none&taskId=ud9e0507d-ce34-4142-90ff-41c3ead30a1&title=&width=1204)
检索字段的mapping类型选择一般都是根据查询场景来决定的。详细参考官网[https://www.elastic.co/guide/en/elasticsearch/reference/7.5/keyword.html](https://www.elastic.co/guide/en/elasticsearch/reference/7.5/keyword.html)
### 5.3、线上零停机重构mapping
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1719021652608-cb565665-00c5-48fc-8627-0fe4ed470cee.png#averageHue=%23f8f8f8&clientId=ue7175eab-f5e6-4&from=paste&height=387&id=u2690053b&originHeight=774&originWidth=1434&originalType=binary&ratio=2&rotation=0&showTitle=false&size=206712&status=done&style=none&taskId=u076bced2-6bbc-4a52-8f59-0b8b8219975&title=&width=717)
有了上面的介绍，结论是只对需要检索的字段建立mapping，再根据检索的场景选择对应的mapping类型。
我们这边遇到的挑战首先是众多的mapping字段数量（300+）以及多条不同业务（10+）的检索诉求，怎么确保重构后的mapping索引覆盖线上的检索诉求？
我们是这样做的，先是基于核心链路使用的检索条件梳理出初版的检索字段，其次是线上变更稳定性三板斧可灰度、可回滚、可监控的方案最终完成整个索引重构。灰度过程中，如果没有覆盖的索引会导致检索报错，通过告警监控可以及时发现，直接回滚到旧mapping。
## 六、效果回收
mapping瘦身后的结果
1、74（282->74）个复杂类型索引（text、integer、float、date、geo_point等)+ 59(67->59)个keyword
2、错用的数据类型（integer->keyword） 10个
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718698247657-068ca97a-fe47-445c-93e3-46ef95981ad5.png#averageHue=%23f0f3f7&clientId=ua743a0cd-6469-4&from=paste&height=223&id=TS4Sa&originHeight=1004&originWidth=1478&originalType=binary&ratio=1&rotation=0&showTitle=false&size=168865&status=done&style=none&taskId=u08a05a98-022e-4bb7-ad70-f367e013b85&title=&width=328)![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718698531151-f882470f-bc86-4e54-9778-caefd2561deb.png#averageHue=%23f0f3f7&clientId=ua743a0cd-6469-4&from=paste&height=221&id=p11vW&originHeight=1024&originWidth=1512&originalType=binary&ratio=1&rotation=0&showTitle=false&size=170735&status=done&style=none&taskId=uaed4b973-287a-422b-9eb1-88b6054a03a&title=&width=326)

可以看到优化后，集群高峰期平均cpu水位50%，日常平均cpu水位20%（60%->20%）
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1718871682515-cadc7fe4-1a13-4d29-897f-a7479d800d02.png#averageHue=%2325282d&clientId=u3f0eb598-aeb4-4&from=paste&height=72&id=u9cd05b7b&originHeight=144&originWidth=1647&originalType=binary&ratio=2&rotation=0&showTitle=false&size=84760&status=done&style=none&taskId=u06573dc6-884d-48a2-89c5-18f0130e592&title=&width=823.5)
flink数据同步任务最大延迟10min（1.5小时->10min）
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1719803116420-ae31092d-f66a-42eb-baab-42a550542bf9.png#averageHue=%23191d21&clientId=u430da192-b689-4&from=paste&height=322&id=u07ed0e9e&originHeight=322&originWidth=1981&originalType=binary&ratio=1&rotation=0&showTitle=false&size=190410&status=done&style=none&taskId=uac6af842-0d61-4a90-9edf-d4efb05bde9&title=&width=1981)
查询耗时峰值150ms(1s->150ms)  平均14.5ms(150ms->14.5ms)
![image.png](https://cdn.nlark.com/yuque/0/2024/png/758294/1719803129225-dbcd3448-c01e-4b59-8474-74769f537cb7.png#averageHue=%23202429&clientId=u430da192-b689-4&from=paste&height=178&id=u5b9b2d63&originHeight=178&originWidth=1198&originalType=binary&ratio=1&rotation=0&showTitle=false&size=67499&status=done&style=none&taskId=u3e780533-4db0-4683-ac05-11a2aefce6e&title=&width=1198)
缩容两台机器(30->28)，回收成本年化1.6w （680*2*12）
## 七、参考
[https://www.elastic.co/cn/blog/found-elasticsearch-from-the-bottom-up](https://www.elastic.co/cn/blog/found-elasticsearch-from-the-bottom-up)
[https://www.elastic.co/guide/en/elasticsearch/guide/current/merge-process.html](https://www.elastic.co/guide/en/elasticsearch/guide/current/merge-process.html)
 [https://www.elastic.co/blog/lucene-points-6-0](https://www.elastic.co/blog/lucene-points-6-0)
[https://elasticsearch.cn/article/446](https://elasticsearch.cn/article/446) 
[https://tech.hellobike.com/#/article/3006129730349957139](https://tech.hellobike.com/#/article/3006129730349957139)

