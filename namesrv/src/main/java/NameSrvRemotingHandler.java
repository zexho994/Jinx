import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import netty.server.NettyServerHandler;

/**
 * @author Zexho
 * @date 2021/12/15 3:59 PM
 */
@Log4j2
public class NameSrvRemotingHandler extends NettyServerHandler {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.info("[NameServer] command => {}", msg);
    }

}
