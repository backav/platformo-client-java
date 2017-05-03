package li.xiangyang.platformo.client;

import com.alibaba.fastjson.JSON;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by bac on 2017/4/14.
 */
public class PlatformoClient {

    private final String endpoint;
    private final String username;
    private final String password;
    private final int interval;

    private Executor executor;

    private Map<String, Long> pathCollectCache = new HashMap<String, Long>();


    public PlatformoClient(String endpoint, String username, String password, int interval, int threadCount) {
        this.endpoint = endpoint;
        this.username = username;
        this.password = password;
        this.interval = interval;
        executor = Executors.newFixedThreadPool(threadCount,
                new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        Thread t = Executors.defaultThreadFactory().newThread(r);
                        t.setName("PlatformoClient-"+t.getName());
                        t.setDaemon(true);
                        return t;
                    }
                });
    }

    public void collect(String remote_addr,
                        String host_addr,
                        String x_forwarded_for,
                        String path,
                        String query,
                        String method,
                        int response_duration,
                        String request_utc_time,
                        int status) {

        long now = System.currentTimeMillis();
        if (this.pathCollectCache.containsKey(path)) {
            long last = this.pathCollectCache.get(path);
            if (now - last < interval) {
                return;
            }
        }
        this.pathCollectCache.put(path, now);

        final Map<String, Object> message = new HashMap<String, Object>();
        message.put("remote_addr", remote_addr);
        message.put("host_addr", host_addr);
        message.put("x_forwarded_for", x_forwarded_for);
        message.put("path", path);
        message.put("query", query);
        message.put("method", method);
        message.put("response_duration", response_duration);
        message.put("request_utc_time", request_utc_time);
        message.put("status", status);

        executor.execute(new Runnable() {
            public void run() {
                try {
                    HttpResponse<String> res = Unirest.post(endpoint).header("Content-Type", "application/json")
                            .basicAuth(username, password).body(JSON.toJSONString(message)).asString();
                    if (res.getStatus() != 200) {
                        System.out.print("Fail to Save:" + res.getStatus());
                    }
                } catch (UnirestException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private String generateAuth() {
        String base64_string = null;
        try {
            base64_string = Base64.encodeBase64String((username + ":" + password).getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "Basic " + base64_string;
    }


}
