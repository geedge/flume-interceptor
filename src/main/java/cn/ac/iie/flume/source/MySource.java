package cn.ac.iie.flume.source;

import cn.ac.iie.flume.bean.NewConn;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * @ClassNameMySource
 * @Author lixkvip@126.com
 * @Date2020/7/28 18:29
 * @Version V1.0
 **/
public class MySource extends AbstractSource implements Configurable, PollableSource {


    @Override
    public Status process() throws EventDeliveryException {

        NewConn newConn = new NewConn();


        return null;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {

    }
}
