package producer;

import message.Message;

/**
 * 用于事务处理的接口
 *
 * @author Zexho
 * @date 2022/1/4 5:55 PM
 */
public interface TransactionListener {

    /**
     * 执行本地事务
     *
     * @param msg 消息对象
     * @return 本地事务的执行结果
     */
    LocalTransactionState executeLocalTransaction(final Message msg);

    /**
     * 检查事务的状态，给回查时使用
     *
     * @param msg 消息对象
     * @return 本地事务的执行结果
     */
    LocalTransactionState checkLocalTransaction(final Message msg);

}
