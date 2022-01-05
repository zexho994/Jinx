package producer;

import message.Message;
import message.TopicRouteInfo;
import netty.common.RemotingCommandHelper;
import netty.protocal.RemotingCommand;

/**
 * 使用事务的生产者对象
 *
 * @author Zexho
 * @date 2022/1/4 5:32 PM
 */
public class TransactionMQProducer extends Producer {

    private TransactionListener transactionListener;

    public TransactionMQProducer(String host) {
        super(host);
    }

    /**
     * 发送事务消息
     *
     * @param message 要发送的消息对象
     */
    @Override
    public void sendMessage(Message message) throws Exception {
        if (this.transactionListener == null) {
            throw new Exception("the transaction listener is null");
        }
        // 标记为half消息
        RemotingCommand command = new RemotingCommand();
        RemotingCommandHelper.markHalf(command);
        // 选择发送队列
        TopicRouteInfo tf = namesrvService.getTopicRouteInfo(message.getTopic());
        ensureBrokerConnected(tf);
        if (message.getQueueId() == null) {
            message.setQueueId((int) (System.currentTimeMillis() % tf.getQueueNum()) + 1);
        }
        // 发送half消息
        RemotingCommand resp = brokerRemoteManager.sendSync(tf.getBrokerName(), command);
        // 执行本地事务接口
        this.transactionListener.executeLocalTransaction(message);
        // 发送end消息
    }

    public void setTransactionListener(TransactionListener listener) {
        this.transactionListener = listener;
    }

}
