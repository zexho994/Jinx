package producer;

import lombok.extern.log4j.Log4j2;
import message.Message;
import message.MessageRequest;

import java.util.PriorityQueue;

/**
 * @author Zexho
 * @date 2021/12/10 5:17 下午
 */
@Log4j2
public class MessageRequestTable {

    /**
     * 最大重试次数
     */
    private static final int MAX_RETRY_COUNT = 3;
    /**
     * 基于发送时间的优先级队列
     */
    private final PriorityQueue<MessageRequest> requestQueue;
    private final Producer producer;

    public MessageRequestTable(Producer producer) {
        this.requestQueue = new PriorityQueue<>(16, (o1, o2) -> (int) (o1.getSendDate() - o2.getSendDate()));
        this.producer = producer;
    }

    public void offer(Message message) {
        if (this.get(message.getTransactionId()) == null) {
            MessageRequest sm = new MessageRequest(message);
            requestQueue.offer(sm);
        }
    }

    /**
     * 移除请求对象
     *
     * @param msgId
     */
    public MessageRequest remove(String msgId) {
        MessageRequest msg = this.get(msgId);
        if (msg == null) {
            return null;
        }
        requestQueue.remove(msg);
        return msg;
    }

    /**
     * 获取请求的重试次数
     *
     * @param msgId
     * @return
     */
    public int getRetryCount(String msgId) {
        MessageRequest messageRequest = this.get(msgId);
        return messageRequest.getRetryCount();
    }

    /**
     * 获取请求对象
     *
     * @param msgId
     * @return
     */
    public MessageRequest get(String msgId) {
        MessageRequest messageRequest = requestQueue.stream().filter(req -> req.getMessage().getTransactionId().equals(msgId)).findFirst().orElse(null);
        if (messageRequest == null) {
            log.warn("MessageRequestTable get message fail, msgId {} not found", msgId);
        }
        return messageRequest;
    }

    /**
     * 进行请求重试
     *
     * @param msgId
     */
    public void retryRequest(String msgId) {
        MessageRequest messageRequest = this.get(msgId);
        int retryCount = messageRequest.getRetryCount();
        if (retryCount >= MAX_RETRY_COUNT) {
            // 调用用户定义的处理方法
            log.info("the retry count reached the upper limit. msgId = {}", msgId);
            this.remove(msgId);
            this.producer.afterRetryProcess.process(messageRequest.getMessage());
        } else {
            messageRequest.incRetryCount();
            messageRequest.setSendDate(System.currentTimeMillis());
            this.producer.sendMessage(messageRequest.getMessage());
        }
    }

    public void scanTimeoutRequest() {
        this.requestQueue.forEach(System.out::println);
    }

}
