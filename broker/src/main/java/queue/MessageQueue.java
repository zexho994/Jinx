package queue;

import Message.Message;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zexho
 * @date 2021/11/19 10:09 上午
 */
public class MessageQueue {

    private final String topic;
    private final String consumerGroup;
    private final Queue<Message> queue = new LinkedBlockingQueue<>();

    public MessageQueue(String topic, String consumerGroup) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public void put(Message message) {
        if (!this.queue.offer(message)) {
            throw new RuntimeException("MessageQueue offer message error");
        }
    }

    public Message poll() {
        return queue.poll();
    }

    public String consumerGroup() {
        return this.consumerGroup;
    }

    public String topic() {
        return this.topic;
    }

}
