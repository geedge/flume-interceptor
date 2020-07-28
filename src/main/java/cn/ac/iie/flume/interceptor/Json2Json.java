package cn.ac.iie.flume.interceptor;


import cn.ac.iie.flume.bean.NewConn;
import cn.ac.iie.flume.bean.OldConn;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

public class Json2Json implements Interceptor {


    //打印日志，便于测试方法的执行顺序
    private static final Logger logger = LoggerFactory.getLogger(Json2Json.class);

    private static byte[] body;

    private static String json;

    @Override
    public void initialize() {
        logger.info("----------自定义拦截器close方法开始执行");
    }

    @Override
    public Event intercept(Event event) {
        body = event.getBody();
        json = new String(body);

        OldConn oldConn = JSON.parseObject(json, OldConn.class);

        NewConn newConn = getNewConn(oldConn);

        body = JSON.toJSONString(newConn).getBytes();

        event.setBody(body);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> results = new ArrayList<>();
        Event event;
        for (Event e : events) {

            event = intercept(e);
            if (event != null) {
                results.add(event);
            }
        }
        return results;
    }

    @Override
    public void close() {
        logger.info("----------自定义拦截器close方法结束执行");
    }

    public static NewConn getNewConn(OldConn oldConn){

        NewConn newConn = new NewConn();

        newConn.setCommon_policy_id(oldConn.getCfg_id());
        newConn.setCommon_start_time(oldConn.getFound_time());
        newConn.setCommon_recv_time(oldConn.getFound_time());
        newConn.setCommon_schema_type(oldConn.getTrans_proto());
        newConn.setCommon_address_type(oldConn.getAddr_type());
        newConn.setCommon_client_ip(oldConn.getD_ip());
        newConn.setCommon_server_ip(oldConn.getS_ip());
        newConn.setCommon_client_port(oldConn.getD_port());
        newConn.setCommon_server_port(oldConn.getS_port());
        newConn.setCommon_encapsulation(oldConn.getEncap_type());
        newConn.setCommon_direction(oldConn.getDirection());

        newConn.setCommon_stream_dir(oldConn.getStream_dir());

        newConn.setCommon_sled_ip(oldConn.getCap_ip());

        newConn.setCommon_address_list(oldConn.getAddr_list());

        newConn.setCommon_c2s_pkt_num(oldConn.getC2s_pkt_num());

        newConn.setCommon_s2c_pkt_num(oldConn.getS2c_pkt_num());

        newConn.setCommon_c2s_byte_num(oldConn.getC2s_byte_num());

        newConn.setCommon_s2c_byte_num(oldConn.getS2c_byte_num());
        return newConn;
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            logger.info("----------build方法执行");
            return new Json2Json();
        }

        @Override
        public void configure(Context context) {
            logger.info("----------configure方法执行");
        }
    }

    public static void main(String[] args) {

    }

}
