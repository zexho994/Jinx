package netty.common;

import enums.ClientType;
import enums.MessageType;
import message.Message;
import message.PropertiesKeys;
import message.RegisterConsumer;
import netty.protocal.RemotingCommand;

/**
 * @author Zexho
 * @date 2021/12/28 3:12 PM
 */
public class RemotingCommandFactory {

    /**
     * 消费者发送消息
     *
     * @param message
     * @return
     */
    public static RemotingCommand putMessage(Message message) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setBody(message);
        remotingCommand.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        remotingCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message.type);
        return remotingCommand;
    }

    /**
     * 注册消费者
     *
     * @return
     */
    public static RemotingCommand registerConsumer(int cid, String topic, String group) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Consumer.type);
        remotingCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Register_Consumer.type);
        Message message = new Message();
        RegisterConsumer registerConsumer = new RegisterConsumer(cid, topic, group);
        message.setBody(registerConsumer);
        remotingCommand.setBody(message);
        return remotingCommand;
    }

    /**
     * 执行消息推送,broker -> consumer
     *
     * @param message 消息体
     * @return
     */
    public static RemotingCommand messagePush(Message message) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Broker.type);
        remotingCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Push_Message.type);
        remotingCommand.setBody(message);
        return remotingCommand;
    }

    public static RemotingCommand putMessageResp(Message msg, int resp) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Broker.type);
        remotingCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message_Resp.type);
        remotingCommand.addProperties(PropertiesKeys.RESP_DATA, String.valueOf(resp));
        remotingCommand.setBody(msg);
        return remotingCommand;

    }
}
