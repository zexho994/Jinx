package queue;

import Message.Message;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zexho
 * @date 2021/11/19 10:09 上午
 */
public class ConsumeQueue {

    private final Queue<Message> queue = new LinkedBlockingQueue<>();

    public void putMessage(Message message) {
        if (!this.queue.offer(message)) {
            throw new RuntimeException("ConsumeQueue offer message error");
        }
    }

    public Message pollMessage() {
        return queue.poll();
    }

}
