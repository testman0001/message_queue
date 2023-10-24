# nsq源码阅读

版本：master分支 2023/09/22

### nsq服务器主要分为两部分：

nsqd，核心进程，接收并投递消息，并将信息反馈给nsqlookupd

nsqlookupd，管理集群信息，提供注册和服务发现功能

### 和传统消息组件一样，按照消息的投递和接收将客户端侧分为生产者和消费者，一条消息生命周期为：

producer->topic->channel->consumer

topic类似于rabbitmq里的fanout exchange，进入的消息会复制到每个channel

channel可被多个消费者消费，但是同一条消息只会给一个消费者（采用负载均衡策略）

注意topic和channel都会独立缓存消息，以避免某个消费者消费太慢导致topic缓冲区不够

### 集群和灾备问题

nsqd本身是单点的，通过nsqlookupd同时提供多个nsqd节点以供消费者使用，关键有以下缺陷：

1.nsqd之间没有互相备份，这个是最不能接受的，一台机器down了那消息就彻底丢了，其他消息中间件就能做到，如rmq、kafka

2.消息持久化触发时机，一般是正常退出或者内存chan达到最大触发，如果没写入磁盘那么消息也会丢失

3.生产者动态发现nsqd问题，只有消费者能通过nsqlookupd动态连nsqd，非常别扭。
如果不先找到topic在哪个实例上，直接在其他nsqd上又创建topic，此时消费者会连上所有有该topic的nsqd实例消费。
假设在脑裂场景，旧topic所在nsqd失联了，producer通过haproxy跳到可用节点上重新投递，那么脑裂恢复后可能两边都有数据，且旧消息再次被消费从而引起其他问题。
个人觉得最好是nsqd集群内部互相备份，就算按照分区来备份也行，生产者根据topic动态找nsqd，消费者同一时间也只去连一个nsqd消费，这样可用性和可靠性都有保障

### 目前看github上有赞自己改了一套，参照kafka实现进行改造，很值得参考：

https://github.com/youzan/nsq

他们大体上怎么解决上述问题的：

1.持久化问题，topic中的消息是使用chan+磁盘队列存储，现在改成任何数据直接落盘，而且不再复制到channel，channel通过记录偏移来判断消费到了哪个位置，另外使用异步pub+批量提交刷盘降低开销

2.引入副本和高可用机制，他们将topic元数据写到etcd（分布式键值存储系统），每个topic由nsqlookupd选举出leader，让follower和其保持同步，生产者先从nsqlookupd查到topic的master节点，
然后再连上去写入，底层使用类似mysql的半同步复制，保证同步到其他follower再返回写入成功。

### 整体上nsq有意思的点：

1.golang天然的并发和chan通信机制，使得整体代码逻辑很简练

2.处理中消息使用的是小根堆实现的优先队列进行存储，很多操作是logN的

3.消费者是同时连接上所有有消费目标topic的nsqd进行消费，而不是传统的一个连接，因此消费性能上可能有优势
