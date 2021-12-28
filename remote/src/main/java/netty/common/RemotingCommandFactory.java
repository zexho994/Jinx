package netty.common;

import enums.ClientType;
import enums.MessageType;
import message.Message;
import message.PropertiesKeys;
import netty.protocal.RemotingCommand;
import utils.ByteUtil;

/**
 * @author Zexho
 * @date 2021/12/28 3:12 PM
 */
public class RemotingCommandFactory {

    public static RemotingCommand putMessage(Message message) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setBody(ByteUtil.to(message));
        remotingCommand.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        remotingCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message.type);
        return remotingCommand;
    }

}
