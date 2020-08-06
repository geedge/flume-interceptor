package cn.ac.iie.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class File2Json implements Interceptor {

    //打印日志，便于测试方法的执行顺序
    private static final Logger logger = LoggerFactory.getLogger(File2Json.class);

    private static byte[] body;

    private static String json;



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


            logger.info( "" + headers.keySet().size());
            for (String key : headers.keySet()) {

                logger.info("[ key: " + key +"," + "value: " + headers.get(key) + "]");
            }

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
            return new File2Json();
        }

        @Override
        public void configure(Context context) {
            logger.info("----------configure方法执行");
        }
    }


    public static void main(String[] args) {

    }
}
