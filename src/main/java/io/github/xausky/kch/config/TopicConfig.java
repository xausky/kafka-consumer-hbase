package io.github.xausky.kch.config;

import java.util.Properties;

/**
 * Created by xausky on 10/26/16.
 */
public class TopicConfig {
    private String topic;
    private String table;
    private int threadCount;
    public TopicConfig(Properties props,String topic){
        this.topic = topic;
        this.table = props.getProperty("io.github.xausky.kch.topic."+topic+".table",topic);
        this.threadCount = Integer.parseInt(props.getProperty("io.github.xausky.kch.topic."+topic+".thread.count","1"));
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
