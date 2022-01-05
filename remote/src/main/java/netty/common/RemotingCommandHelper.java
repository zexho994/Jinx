package netty.common;

import enums.ClientType;
import message.PropertiesKeys;
import netty.protocal.RemotingCommand;

/**
 * @author Zexho
 * @date 2022/1/5 8:16 AM
 */
public class RemotingCommandHelper {

    public static void markHalf(RemotingCommand command) {
        command.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        command.addProperties(PropertiesKeys.TRAN, "true");
    }

}
