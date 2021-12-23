package message;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Zexho
 * @date 2021/12/22 7:23 PM
 */
@Data
public class ConfigBody implements Serializable {
    private static final long serialVersionUID = 6708065748466902809L;

    private List<TopicUnit> topics;

}
