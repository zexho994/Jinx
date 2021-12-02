package store;

import Message.Message;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/2 2:34 下午
 */
public class InnerMessage implements Serializable {

    public final int totalSize;
    public final Message message;

    public InnerMessage(Message message) {
        this.message = message;
        this.totalSize = message.toString().length();
    }

}
