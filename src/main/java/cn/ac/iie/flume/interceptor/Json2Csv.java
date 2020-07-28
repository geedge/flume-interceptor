package cn.ac.iie.flume.interceptor;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.json.CDL;
import org.json.JSONArray;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Json2Csv implements Interceptor{

    //打印日志，便于测试方法的执行顺序
    private static final Logger logger = LoggerFactory.getLogger(Json2Csv.class);

    private static byte[] body;

    private static String json;


    /**
     * Json数据转换为Csv数据
     * @param json
     * @return
     */
    public static String Json2CsvUtils(String json) {
        JSONArray jsonArray = new JSONArray(json);
        String csv =CDL.toString(jsonArray);
        return csv;
    }

    @Override
    public void initialize() {
        logger.info("----------自定义拦截器的initialize方法执行");
    }

    @Override
    public Event intercept(Event event) {
        logger.info("----------intercept(Event event)方法执行，处理单个event");

    //TODO 处理单个event的代码

        body = event.getBody();

        json= "[" + new String(body) + "]";


        String csv = Json2CsvUtils(json);

        String[] split = csv.split("\n");

        body = split[1].getBytes();
        event.setBody(body);
        return event;
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

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            logger.info("----------build方法执行");
            return new Json2Csv();
        }

        @Override
        public void configure(Context context) {
            logger.info("----------configure方法执行");
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
//       String jsonstr = "[{\"common_server_ip\":\"211.90.25.35\",\"common_client_ip\":\"192.168.50.6\",\"common_server_port\":443,\"common_client_port\":59801,\"common_stream_dir\":3,\"common_address_type\":4,\"common_s2c_pkt_num\":97,\"common_s2c_byte_num\":107582,\"common_c2s_pkt_num\":65,\"common_c2s_byte_num\":1370,\"common_start_time\":1589795595,\"common_end_time\":1589795725,\"common_con_duration_ms\":130000,\"common_stream_trace_id\":8113956949548,\"common_l4_protocol\":\"IPv4_TCP\",\"common_address_list\":\"59801-443-192.168.50.6-211.90.25.35\",\"common_schema_type\":\"SSL\",\"common_has_dup_traffic\":0,\"ssl_sni\":\"p4.music.126.net\",\"ssl_pinningst\":0,\"ssl_intercept_state\":1,\"ssl_server_side_latency\":29,\"ssl_client_side_latency\":192,\"ssl_server_side_version\":\"TLSv1.2\",\"ssl_client_side_version\":\"TLSv1.2\",\"ssl_cert_verify\":1,\"common_sled_ip\":\"192.168.40.161\",\"common_policy_id\":14385,\"common_service\":0,\"common_action\":2},\n" +
//               "{\"common_schema_type\":\"SSL\",\"ssl_sni\":\"docs.geedge.net\",\"common_server_ip\":\"47.93.59.84\",\"common_client_ip\":\"192.168.50.2\",\"common_server_port\":443,\"common_client_port\":61636,\"common_stream_dir\":3,\"common_address_type\":4,\"common_s2c_pkt_num\":2,\"common_s2c_byte_num\":0,\"common_c2s_pkt_num\":4,\"common_c2s_byte_num\":520,\"common_start_time\":1589795768,\"common_end_time\":1589795769,\"common_con_duration_ms\":1000,\"common_stream_trace_id\":8113962657521,\"common_l4_protocol\":\"IPv4_TCP\",\"common_address_list\":\"61636-443-192.168.50.2-47.93.59.84\",\"common_sled_ip\":\"192.168.40.161\",\"common_policy_id\":1,\"common_service\":0,\"common_action\":128}]";
//      String jsonstr = "[{\"common_action\":48,\"common_address_type\":4,\"common_client_asn\":\"N/A\",\"common_client_ip\":\"192.168.50.2\",\"common_client_location\":\"Private IP\",\"common_client_port\":61545,\"common_device_id\":\"0\",\"common_direction\":0,\"common_end_time\":1589795698,\"common_entrance_id\":0,\"common_l4_protocol\":\"IPv4_TCP\",\"common_link_id\":0,\"common_log_id\":117046227744456704,\"common_policy_id\":13840,\"common_recv_time\":1589795698,\"common_schema_type\":\"HTTP\",\"common_server_asn\":\"4808\",\"common_server_ip\":\"123.125.82.158\",\"common_server_location\":\"China\",\"common_server_port\":443,\"common_service\":0,\"common_sled_ip\":\"192.168.40.162\",\"common_start_time\":1589795698,\"common_stream_dir\":3,\"common_stream_trace_id\":8113956752685,\"common_sub_action\":\"monitor\",\"common_subscriber_id\":\"test2\",\"http_content_type\":\"text/html\",\"http_cookie\":\"__huid=11n3zx+KGEiggdircvQvnRhx24Z4pP+YuVpz2oC1+OIF4=\",\"http_domain\":\"360.cn\",\"http_host\":\"browser.360.cn\",\"http_request_line\":\"GET browser.360.cn/scene/total_plugin_hosts.html HTTP/2.0\",\"http_response_body\":\"http://192.168.40.223:9090/proxybucket/a6/42/cbeb56454318bd62c51699a53629a10159c6fad2f947b0bcd896a8c6799e\",\"http_response_line\":\"HTTP/2.0 200 OK\",\"http_url\":\"browser.360.cn/scene/total_plugin_hosts.html\",\"http_user_agent\":\"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36 QIHU 360SE\",\"http_version\":\"http2\"}]";
String jsonstr = "[{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"50615-443-37.150.10.41-96.46.176.133\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":1385,\"common_c2s_pkt_num\":5,\"common_client_asn\":\"7979\",\"common_client_ip\":\"96.46.176.133\",\"common_client_location\":\"Ashburn,Virginia,美国\",\"common_client_port\":443,\"common_con_duration_ms\":0,\"common_direction\":1,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825951637508,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":5121,\"common_s2c_pkt_num\":8,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"9198\",\"common_server_ip\":\"37.150.10.41\",\"common_server_location\":\"哈萨克斯坦\",\"common_server_port\":50615,\"common_service\":0,\"common_sled_ip\":\"10.4.4.3\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0},\n" +
        "{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"15947-80-85.117.117.44-54.254.192.56\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":506,\"common_c2s_pkt_num\":2,\"common_client_asn\":\"38895,16509\",\"common_client_ip\":\"54.254.192.56\",\"common_client_location\":\"Singapore,Singapore,新加坡\",\"common_client_port\":80,\"common_con_duration_ms\":0,\"common_direction\":0,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825951637509,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":1040,\"common_s2c_pkt_num\":2,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"29555\",\"common_server_ip\":\"85.117.117.44\",\"common_server_location\":\"哈萨克斯坦\",\"common_server_port\":15947,\"common_service\":0,\"common_sled_ip\":\"10.4.4.30\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0},\n" +
        "{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"IPv4_TCP<62648-443-5.251.150.58-10.100.100.1>|GRE<NULL>|IPv4<89.218.73.54-78.46.206.144>|MAC<000000000426-18100400033c>\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":3292,\"common_c2s_pkt_num\":6,\"common_client_asn\":\"N/A\",\"common_client_ip\":\"10.100.100.1\",\"common_client_location\":\"-,-,-\",\"common_client_port\":443,\"common_con_duration_ms\":0,\"common_direction\":0,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825951637510,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":17667,\"common_s2c_pkt_num\":16,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"9198\",\"common_server_ip\":\"5.251.150.58\",\"common_server_location\":\"哈萨克斯坦\",\"common_server_port\":62648,\"common_service\":0,\"common_sled_ip\":\"10.4.4.60\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0},\n" +
        "{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"59653-443-95.59.99.58-173.194.122.143\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":1240,\"common_c2s_pkt_num\":8,\"common_client_asn\":\"15169\",\"common_client_ip\":\"173.194.122.143\",\"common_client_location\":\"Mountain View,California,美国\",\"common_client_port\":443,\"common_con_duration_ms\":0,\"common_direction\":1,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825951637511,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":944,\"common_s2c_pkt_num\":9,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"9198\",\"common_server_ip\":\"95.59.99.58\",\"common_server_location\":\"哈萨克斯坦\",\"common_server_port\":59653,\"common_service\":0,\"common_sled_ip\":\"10.4.4.18\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0},\n" +
        "{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"41873-443-212.154.169.132-94.100.180.76\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":1986,\"common_c2s_pkt_num\":5,\"common_client_asn\":\"47764\",\"common_client_ip\":\"94.100.180.76\",\"common_client_location\":\"Moscow,Moskva,俄罗斯\",\"common_client_port\":443,\"common_con_duration_ms\":0,\"common_direction\":0,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825951637512,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":513,\"common_s2c_pkt_num\":2,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"50482,9198\",\"common_server_ip\":\"212.154.169.132\",\"common_server_location\":\"哈萨克斯坦\",\"common_server_port\":41873,\"common_service\":0,\"common_sled_ip\":\"10.4.4.26\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0},\n" +
        "{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"23050-443-92.46.127.82-185.86.139.19\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":4386,\"common_c2s_pkt_num\":9,\"common_client_asn\":\"201081\",\"common_client_ip\":\"185.86.139.19\",\"common_client_location\":\"Paris,Ile-de-France,法国\",\"common_client_port\":443,\"common_con_duration_ms\":0,\"common_direction\":0,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825968414720,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":3634,\"common_s2c_pkt_num\":5,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"9198\",\"common_server_ip\":\"92.46.127.82\",\"common_server_location\":\"哈萨克斯坦\",\"common_server_port\":23050,\"common_service\":0,\"common_sled_ip\":\"10.4.4.59\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0},\n" +
        "{\"bgp_type\":0,\"common_action\":0,\"common_address_list\":\"51500-443-192.168.145.145-10.61.43.190\",\"common_address_type\":4,\"common_app_id\":0,\"common_c2s_byte_num\":1239,\"common_c2s_pkt_num\":3,\"common_client_asn\":\"N/A\",\"common_client_ip\":\"10.61.43.190\",\"common_client_location\":\"-,-,-\",\"common_client_port\":443,\"common_con_duration_ms\":0,\"common_direction\":1,\"common_encapsulation\":0,\"common_end_time\":0,\"common_entrance_id\":0,\"common_has_dup_traffic\":0,\"common_link_id\":0,\"common_log_id\":118459825968414721,\"common_policy_id\":0,\"common_protocol_id\":0,\"common_recv_time\":1589879955,\"common_s2c_byte_num\":3060,\"common_s2c_pkt_num\":5,\"common_schema_type\":\"IPv4_TCP\",\"common_server_asn\":\"N/A\",\"common_server_ip\":\"192.168.145.145\",\"common_server_location\":\"-\",\"common_server_port\":51500,\"common_service\":0,\"common_sled_ip\":\"10.4.4.37\",\"common_start_time\":1547461445,\"common_stream_dir\":3,\"common_stream_trace_id\":0,\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":0,\"dns_nscount\":0,\"dns_qclass\":0,\"dns_qdcount\":0,\"dns_qtype\":0,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":0,\"dns_sub\":0,\"dns_tc\":0,\"http_proxy_flag\":0,\"http_sequence\":0,\"ssl_client_side_latency\":0,\"ssl_con_latency_ms\":0,\"ssl_server_side_latency\":0}]";
        System.out.println("开始转换");
        FileUtils.writeStringToFile(new File("D:/Json2Csv/demo5.csv"), Json2CsvUtils(jsonstr),"UTF-8");
        Thread.sleep(10L);
        System.out.println("转换结束");
    }
}
