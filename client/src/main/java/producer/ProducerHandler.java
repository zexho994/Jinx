package producer;

import enums.MessageResponseCode;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.ProducerMessageResponse;
import netty.client.NettyClientHandler;

/**
 * @author Zexho
 * @date 2021/12/10 5:08 下午
 */
@Log4j2
public class ProducerHandler extends NettyClientHandler {

    private final Producer producer;

    public ProducerHandler(Producer producer) {
        this.producer = producer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.debug("broker's ack => {}", msg);
        ProducerMessageResponse response = (ProducerMessageResponse) msg;
        MessageResponseCode code = MessageResponseCode.get(response.getCode());
        switch (code) {
            case SUCCESS:
                producer.messageRequestTable.remove(response.getTransactionId());
                break;
            case FAILURE:
                producer.messageRequestTable.retryRequest(response.getTransactionId());
                break;
            default:
                log.warn("ProducerMessageResponse code not match, code = {}", response.getCode());
        }
    }
}
