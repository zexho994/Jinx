package producer;

import lombok.extern.log4j.Log4j2;
import message.Message;
import message.TopicRouteInfo;
import netty.common.RemotingCommandHelper;
import netty.protocal.RemotingCommand;

import java.util.concurrent.ExecutionException;

/**
 * 使用事务的生产者对象
 *
 * @author Zexho
 * @date 2022/1/4 5:32 PM
 */
@Log4j2
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
        // 发送half消息
        RemotingCommand resp = this.sendHalfMessage(message);
        // 执行本地事务接口
        this.transactionListener.executeLocalTransaction(message);
        // 发送end消息
        this.sendEndMessage(message);
    }

    public void setTransactionListener(TransactionListener listener) {
        this.transactionListener = listener;
    }

    private RemotingCommand sendHalfMessage(Message message) throws ExecutionException, InterruptedException {
        log.debug("send half message");
        // 标记为half消息
        RemotingCommand command = new RemotingCommand();
        RemotingCommandHelper.markHalf(command);
        // 选择发送队列
        TopicRouteInfo tf = namesrvService.getTopicRouteInfo(message.getTopic());
        ensureBrokerConnected(tf);
        if (message.getQueueId() == null) {
            message.setQueueId((int) (System.currentTimeMillis() % tf.getQueueNum()) + 1);
        }
        command.setBody(message);
        return brokerRemoteManager.sendSync(tf.getBrokerName(), command);
    }

    private void sendEndMessage(Message message) {
        log.info("send end message");

    }

}
