package queue;

import Message.Message;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zexho
 * @date 2021/11/19 10:09 上午
 */
public class MessageQueue {

    private String consumerGroup;
    private final Queue<Message> queue = new LinkedBlockingQueue<>();

    public void put(Message message) {
        if (!consumerGroup.equals(message.getConsumerGroup())) {
            throw new RuntimeException("consumerGroup is not match");
        }
        this.queue.offer(message);
    }

    public Message poll() {
        return queue.poll();
    }

    public String consumerGroup() {
        return consumerGroup;
    }

}
