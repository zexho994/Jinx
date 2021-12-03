package store;

import Message.Message;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/2 2:34 下午
 */
public class InnerMessage implements Serializable {
    private static final long serialVersionUID = 7652172333492482948L;

    public final int totalSize;
    public final Message message;

    public InnerMessage(Message message) {
        this.message = message;
        this.totalSize = message.toString().length();
    }

}
