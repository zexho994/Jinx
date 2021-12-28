package netty.common;

import enums.ClientType;
import enums.MessageType;
import message.Message;
import message.PropertiesKeys;
import message.RegisterConsumer;
import netty.protocal.RemotingCommand;
import utils.ByteUtil;

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
        remotingCommand.setBody(ByteUtil.to(message));
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
        RegisterConsumer registerConsumer = new RegisterConsumer(cid, topic, group);
        remotingCommand.setBody(ByteUtil.to(registerConsumer));
        return remotingCommand;
    }

}
