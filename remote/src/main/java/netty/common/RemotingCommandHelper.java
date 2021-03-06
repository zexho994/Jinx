package netty.common;

import enums.ClientType;
import enums.MessageType;
import message.PropertiesKeys;
import message.TranType;
import netty.protocal.RemotingCommand;

/**
 * @author Zexho
 * @date 2022/1/5 8:16 AM
 */
public class RemotingCommandHelper {

    public static void markHalf(RemotingCommand command) {
        command.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        command.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message.type);
        command.addProperties(PropertiesKeys.TRAN, TranType.Half.type);
    }

    public static void markEnd(RemotingCommand command, String type) {
        command.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        command.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message.type);
        command.addProperties(PropertiesKeys.TRAN, type);
    }
}
