package producer;

import lombok.extern.log4j.Log4j2;
import message.Message;
import message.TopicRouteInfo;
import message.TranType;
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
        // 选择发送队列
        TopicRouteInfo tf = namesrvService.getTopicRouteInfo(message.getTopic());
        ensureBrokerConnected(tf);
        if (message.getQueueId() == null) {
            message.setQueueId((int) (System.currentTimeMillis() % tf.getQueueNum()) + 1);
        }
        // 发送half消息
        RemotingCommand halfResp = this.sendHalfMessage(message, tf);
        // 执行本地事务接口
        log.debug("execute local transaction");
        LocalTransactionState localTransactionState = this.transactionListener.executeLocalTransaction(message);
        // 发送end消息
        log.debug("send end message");
        RemotingCommand endResp = this.sendEndMessage(message, localTransactionState, tf);
    }

    public void setTransactionListener(TransactionListener listener) {
        this.transactionListener = listener;
    }

    private RemotingCommand sendHalfMessage(Message message, TopicRouteInfo tf) throws ExecutionException, InterruptedException {
        log.debug("send half message");
        // 标记为half消息
        RemotingCommand command = new RemotingCommand();
        RemotingCommandHelper.markHalf(command);
        command.setBody(message);
        return brokerRemoteManager.sendSync(tf.getBrokerName(), command);
    }

    private RemotingCommand sendEndMessage(Message message, LocalTransactionState localTransactionState, TopicRouteInfo tf) throws Exception {
        log.info("send end message");
        RemotingCommand command = new RemotingCommand();
        switch (localTransactionState) {
            case COMMIT:
                RemotingCommandHelper.markEnd(command, TranType.Commit.type);
                break;
            case ROLLBACK:
                RemotingCommandHelper.markEnd(command, TranType.Rollback.type);
                break;
            default:
                throw new Exception("local transaction state error. state is " + localTransactionState);
        }
        command.setBody(message);
        return brokerRemoteManager.sendSync(tf.getBrokerName(), command);
    }

}
