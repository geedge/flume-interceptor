package cn.ac.iie.flume.interceptor;

import cn.ac.iie.flume.bean.Connection;
import cn.ac.iie.flume.bean.NewConn;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Csv2Json implements Interceptor {

    //打印日志，便于测试方法的执行顺序
    private static final Logger logger = LoggerFactory.getLogger(Csv2Json.class);

    private static byte[] body;

    private static String json;
    private NewConn newConn;


    @Override
    public void initialize() {
        logger.info("----------自定义拦截器的initialize方法执行");
    }

    @Override
    public Event intercept(Event event) {
        logger.info("----------intercept(Event event)方法执行，处理单个event");

        //TODO 处理单个event的代码
        //取出一条csv格式的数据

        try{
            body = event.getBody();

            Map<String, String> headers = event.getHeaders();

            json = new String(body);

            String[] strings = json.replaceAll("\t"," " ).replaceAll("\"","" ).split(",");

            newConn = new NewConn();

            newConn.setCommon_log_id(Long.parseLong("0"));
            newConn.setCommon_policy_id(Integer.parseInt(strings[1]));
            newConn.setCommon_start_time(Integer.parseInt(strings[2]));
            newConn.setCommon_recv_time(Integer.parseInt(strings[3]));
            newConn.setCommon_l4_protocol(strings[4]);
            newConn.setCommon_address_type(Integer.parseInt(strings[5]));
            newConn.setCommon_client_ip(strings[6]);
            newConn.setCommon_server_ip(strings[7]);
            newConn.setCommon_client_port(Integer.parseInt(strings[8]));
            newConn.setCommon_server_port(Integer.parseInt(strings[9]));
            newConn.setCommon_service(Integer.parseInt(strings[10]));
            newConn.setCommon_entrance_id(Integer.parseInt(strings[11]));
            newConn.setCommon_device_id(strings[12]);
            newConn.setCommon_link_id(Integer.parseInt(strings[13]));
            newConn.setCommon_encapsulation(Integer.parseInt(strings[14]));
            newConn.setCommon_direction(Integer.parseInt(strings[15]));
            newConn.setCommon_stream_dir(Integer.parseInt(strings[18]));
            newConn.setCommon_sled_ip(strings[19]);
            newConn.setCommon_address_list(strings[20]);
            newConn.setCommon_server_location(strings[21]);
            newConn.setCommon_client_location(strings[22]);
            newConn.setCommon_server_asn(strings[23]);
            newConn.setCommon_client_asn(strings[24]);
            newConn.setCommon_subscriber_id(strings[26]);
            newConn.setCommon_user_region(strings[27]);
            newConn.setCommon_c2s_pkt_num(Integer.parseInt(strings[30]));
            newConn.setCommon_s2c_pkt_num(Integer.parseInt(strings[31]));
            newConn.setCommon_c2s_byte_num(Integer.parseInt(strings[32]));
            newConn.setCommon_s2c_byte_num(Integer.parseInt(strings[33]));
            newConn.setCommon_protocol_label(strings[34]);
            newConn.setCommon_app_label(strings[35]);
            newConn.setCommon_action(Integer.parseInt(strings[39]));


            json = JSONObject.toJSONString(newConn);

            body = json.getBytes();
            event.setBody(body);
            return event;
        }
        catch (Exception e){
            logger.error("消息转换异常========");
            e.printStackTrace();
            return null;
        }

    }

    //TODO 处理多个event的函数
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
        logger.info("----------自定义拦截器close方法执行");
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            logger.info("----------build方法执行");
            return new Csv2Json();
        }

        @Override
        public void configure(Context context) {
            logger.info("----------configure方法执行");
        }
    }


    public static void main(String[] args) {

    }
}
