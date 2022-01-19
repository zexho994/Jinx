package ha;

import lombok.extern.log4j.Log4j2;
import model.BrokerData;

/**
 * @author Zexho
 * @date 2022/1/18 5:31 PM
 */
@Log4j2
public class HASlave {

    private HASlave() {
    }

    private static class Inner {
        private static final HASlave INSTANCE = new HASlave();
    }

    public static HASlave getInstance() {
        return HASlave.Inner.INSTANCE;
    }

    private BrokerData masterData = null;

    /**
     * 向master汇报同步进度（即commitlog offset）
     */
    public void startReportOffset() {

    }

    /**
     * 保存master的路由信息，用于建立连接
     *
     * @param masterData
     */
    public void saveMasterRouteData(BrokerData masterData) {
        log.info("save master route data");
        this.masterData = masterData;
    }

}
