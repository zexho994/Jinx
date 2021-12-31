package consumer;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.RegisterConsumer;
import message.TopicUnit;
import netty.common.RemotingCommandFactory;
import store.DefaultMessageStore;
import store.MessageStore;
import store.consumequeue.ConsumeQueue;
import topic.TopicManager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * @author Zexho
 * @date 2021/12/6 10:47 上午
 */
@Log4j2
public class ConsumerManager {

    private ConsumerManager() {
    }

    private static class Inner {
        private static final ConsumerManager INSTANCE = new ConsumerManager();
    }

    public static ConsumerManager getInstance() {
        return ConsumerManager.Inner.INSTANCE;
    }

    private final MessageStore messageStore = DefaultMessageStore.getInstance();
    private final ConsumeQueue consumeQueue = ConsumeQueue.getInstance();
    private final TopicManager topicManager = TopicManager.getInstance();

    private final Map<String, Map<String, Lock>> locks = new ConcurrentHashMap<>();
    private final Map<Integer/*cid*/, ChannelHandlerContext/*ctx*/> channelMap = new ConcurrentHashMap<>();
    private final Map<String/*topic*/, Set<String/*group*/>> topicGroupMap = new ConcurrentHashMap<>();
    private final Map<String/*group*/, Set<Integer/*cid*/>> groupCidsMap = new ConcurrentHashMap<>();
    private final Map<String/*topic*/, Map<String/*group*/, Map<Integer/*queueId*/, Integer/*cid*/>>> topicQueueSubMap = new ConcurrentHashMap<>();

    /**
     * push 定时任务, 每100ms执行一次
     */
    public void startPushTask() {
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::doPush, 1000, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * step1 : 获取待检查消费者列表
     * step2 : 检查队列是否有新消息
     * step3 : 如果有新消息，执行推送，然后更新offset
     */
    private void doPush() {
        this.topicQueueSubMap.keySet().forEach(topic -> {
            topicQueueSubMap.get(topic).keySet().forEach(group -> {
                topicQueueSubMap.get(topic).get(group).keySet().forEach(queue -> {
                    Message message = this.pullMessage(topic, queue, group);
                    if (message != null) {
                        Integer cid = topicQueueSubMap.get(topic).get(group).get(queue);
                        log.info("[PushTask] push message to consumer. cid = {}, topic = {}, group = {}, msgId = {}, msgId ={}", cid, topic, group, queue, message.getTransactionId());
                        ChannelHandlerContext ctx = this.channelMap.get(cid);
                        ctx.writeAndFlush(RemotingCommandFactory.messagePush(message));
                    }
                });
            });
        });
    }

    /**
     * 注册消费者，保存消费者的信息
     *
     * @param registerConsumer 注册消息体
     * @param ctx              消费者的消息通道
     */
    public void registerConsumer(RegisterConsumer registerConsumer, ChannelHandlerContext ctx) {
        log.info("Register consumer. data = {}", registerConsumer);
        // 保存Consumer连接信息
        this.channelMap.put(registerConsumer.getCid(), ctx);
        // 保存 topic-group
        if (!this.topicGroupMap.containsKey(registerConsumer.getTopic())) {
            this.topicGroupMap.put(registerConsumer.getTopic(), new CopyOnWriteArraySet<>());
        }
        this.topicGroupMap.get(registerConsumer.getTopic()).add(registerConsumer.getGroup());
        // 保存group-cid映射
        if (!this.groupCidsMap.containsKey(registerConsumer.getGroup())) {
            this.groupCidsMap.put(registerConsumer.getGroup(), new CopyOnWriteArraySet<>());
        }
        this.groupCidsMap.get(registerConsumer.getGroup()).add(registerConsumer.getCid());
        // 执行消费者重平衡
        rebalanced(registerConsumer.getTopic(), registerConsumer.getGroup());
    }

    /**
     * 推送消息
     * step1: 找到topic的所有消费组
     * step2: 在每个消费组中根据queueId发送消息
     * step3: 更新queue offset
     *
     * @param topic   消息主题
     * @param queueId 消息所属消费队列
     * @param message 消息体
     */
    public void doMessagePush(String topic, int queueId, Message message) {
        Set<String> groups = this.topicGroupMap.get(topic);
        Map<String, Map<Integer, Integer>> groupQueueMap = this.topicQueueSubMap.get(topic);
        if (groups == null || groupQueueMap == null) {
            return;
        }

        groups.forEach(group -> {
            Integer cid = groupQueueMap.get(group).get(queueId);
            this.channelMap.get(cid).writeAndFlush(RemotingCommandFactory.messagePush(message));
        });
    }

    /**
     * 执行消费者的重平衡
     * 何为重平衡：topic下所有queue都要分配给对应的consumer，并且要按照某种规则进行分配。
     * 目前按照循环的方式从头分配 : 队列数量为n，消费者数量c，遍历(1->n), 分配给(0->c)
     * TODO 支持更多的分配方式
     */
    private void rebalanced(String topic, String group) {
        if (!this.topicQueueSubMap.containsKey(topic)) {
            this.topicQueueSubMap.put(topic, new ConcurrentHashMap<>());
        }

        TopicUnit topicUnit = topicManager.getTopic(topic);
        int[] cids = this.groupCidsMap.get(group).stream().flatMapToInt(IntStream::of).toArray();
        Map<Integer, Integer> queueCidMap = new ConcurrentHashMap<>(topicUnit.getQueue());
        for (int i = 1; i <= topicUnit.getQueue(); i++) {
            queueCidMap.put(i, cids[i % cids.length]);
        }
        log.info("TOPIC<{}> GROUP<{}> rebalanced success", topic, group);
        this.topicQueueSubMap.get(topic).put(group, queueCidMap);
    }


    /**
     * 消费消息
     * step1: 获取group在消费队列的消费序号
     * step2: 根据消费序号在消费队列中找到commitlog offset
     * step3: 根据commitlog offset在commitlog文件中找到对象消息
     *
     * @param topic        消息主题
     * @param consumeGroup 消费组
     * @return 未消费的消息
     */
    public Message pullMessage(String topic, int queueId, String consumeGroup) {
        Lock lock = this.getLock(topic, consumeGroup);
        lock.lock();
        try {
            Message message = messageStore.findMessage(topic, queueId, consumeGroup);
            if (message != null) {
                log.debug("find message => {}", message);
                consumeQueue.incOffset(topic, queueId, consumeGroup);
            }
            return message;
        } finally {
            lock.unlock();
        }
    }

    private Lock getLock(String topic, String consumeGroup) {
        // 获取锁
        Map<String, Lock> topicLocks = locks.get(topic);
        Lock lock;
        if (topicLocks == null) {
            topicLocks = new ConcurrentHashMap<>(4);
            locks.put(topic, topicLocks);
            lock = new ReentrantLock();
            topicLocks.put(consumeGroup, lock);
        } else {
            lock = topicLocks.get(consumeGroup);
            if (lock == null) {
                lock = new ReentrantLock();
                topicLocks.put(consumeGroup, lock);
            }
        }
        return lock;
    }
}
