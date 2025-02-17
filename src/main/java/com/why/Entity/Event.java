package com.why.Entity;

/**
 * Created by WHY on 2025/2/17.
 * Functions:
 */
import java.sql.Timestamp;

// 记录用户访问某个url这一事件
public class Event {
    public String user; //用户
    public String url; //网站url
    public Long timestamp; //时间戳

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
