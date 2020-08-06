/**
 * @ClassNameEventTest
 * @Author lixkvip@126.com
 * @Date2020/7/29 15:38
 * @Version V1.0
 **/
public class EventTest {

    public static void main(String[] args) {

        String json = "\"0e1178ab-986f-4bbb-91f9-94b1f70ce3a2\",0,1546624800,1546624800,\"IPv4_UDP\",0,\"37.150.25.154\",\"46.20.187.219\",32265,44985,160,2,0,0,0,0,\"\",\"\",1,\"10.3.4.10\",\"48975-14441-46.20.187.219-37.150.25.154\",\"\",\"\",\"42322\",\"9198\",\"\",\"\",\"\",\"\",\"PROTO_ID=0;APP_ID=0;OS_ID=0;BS_ID=0;WEB_ID=0;BEHAV_ID=0;\",3,0,60,0,0,0,0,0,0,0";

        String[] strings = json.replaceAll("\t"," " ).replaceAll("\"","" ).split(",");

        System.out.println(strings[29]);
        System.out.println(strings[28]);
        System.out.println(strings[30]);


    }
}
