package producer;

import message.Message;
import org.junit.jupiter.api.Test;
import utils.ByteUtil;

class TransactionMQProducerTest {

    TransactionMQProducer producer;

    void startTransactionMQProducer() {
        producer = new TransactionMQProducer("127.0.0.1");
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg) {
                System.out.println("[Test] executeLocalTransaction, message => " + msg);
                return LocalTransactionState.UNKNOWN;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(Message msg) {
                System.out.println("[Test] checkLocalTransaction, message => " + msg);
                return LocalTransactionState.UNKNOWN;
            }
        });
        producer.start();
    }

    {
        startTransactionMQProducer();
    }

    @Test
    public void sendMessage() throws Exception {
        Message message = new Message();
        message.setTopic("topic_1");
        message.setBody(ByteUtil.to(10001));

        this.producer.sendMessage(message);
    }
}