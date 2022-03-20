# 运行示例代码
## 运行Namesrv
namesrv监听端口默认为9876
```java
public static void main(String[] args) {
    NameSrvRemoting nameSrvRemoting = new NameSrvRemoting();
    nameSrvRemoting.start();
}
```

启动成功，控制台打印：
![](https://tva1.sinaimg.cn/large/e6c9d24egy1h0gnbooxvmj21t802ijsc.jpg)

## 运行Broker主节点
启动参数
```
-N 127.0.0.1 -B 127.0.0.1 -n slave -bcp /Users/zexho/jinx/config/masterConfig -p 9944 -scp /Users/zexho/jinx/config/masterStoreConfig
```

- -N：nameserver服务的共有ip地址。如果为集群，传入所有的ip地址
- -B：broker服务的公有ip地址
- -n：broker实例名称
- -bcp：broker配置文件路径
- -p：监听端口
- -scp：store配置文件路径

启动的是Master/Slave 取决于配置文件中设置的brokerId，为0代表Master，其他表示Slave，下面是一个参考。
```json
{
  "clusterName":"cluster_test_A",
  "brokerName":"broker_name_a",
  "brokerId":0,
  "body":{
    topics:[
    {
      "topic":"topic_1",
      "queue":4
    },
    {
      "topic":"topic_2",
      "queue":6
    },
    {
      "topic":"topic_3",
      "queue":2
    }
    ]
  }
}
```
```java
public static void main(String[] args) throws Exception {
    BrokerStartup brokerStartup = new BrokerStartup();
    brokerStartup.start0(args);
}
```
## 运行Broker从节点
启动Slave的方式与Master类似，将BrokerId 设置为非0即可。

##运行Producer
```java
// 设置broker地址
DefaultMQProducer defaultMQProducer = new DefaultMQProducer("127.0.0.1");
// 启动
defaultMQProducer.start();

// 消息发送单位 message
Message message = new Message(UUID.randomUUID().toString());
// 设置消息topic
message.setTopic("topic_1");
// 发送
defaultMQProducer.sendMessage(message);
```

## 运行Consumer

```java
public static void startConsumer(String topic, String group, int cid, Map<String, Integer> msgMap) {
    // 设置broker的地址，客户端id（业务方保证唯一）
    Consumer consumer = new Consumer("127.0.0.1", 39182);
    // 设置订阅的topic
    consumer.setTopic("topic_1");
    // 设置消费组
    consumer.setConsumerGroup("consumer_group");
    // 设置消息处理
    consumer.setConsumerListener(msg -> {
        System.out.println("msg = " + msg);
    });
    // 启动
    consumer.start();
}
```
