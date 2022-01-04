package producer;

import message.Message;

/**
 * 使用事务的生产者对象
 *
 * @author Zexho
 * @date 2022/1/4 5:32 PM
 */
public class TransactionMQProducer extends Producer {

    public TransactionMQProducer(String host) {
        super(host);
    }

    @Override
    public void sendMessage(Message message) {

    }


}
