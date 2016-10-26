package io.github.xausky.kch.config;

import java.util.Properties;

/**
 * Created by xausky on 10/26/16.
 */
public class Config {
    private int port;
    private String[] topics;
    private String groupId;
    private String zookeeper;
    private String columnFamily;
    private String bootstrapServer;

    public Config(Properties props){

        topics = props.getProperty("io.github.xausky.kch.topics","test").split(",");
        groupId = props.getProperty("io.github.xausky.kch.group.id","kch");
        port = Integer.parseInt(props.getProperty("io.github.xausky.kch.zookeeper.port","2181"));
        bootstrapServer = props.getProperty("io.github.xausky.kch.bootstrap.server","localhost:9092");
        zookeeper = props.getProperty("io.github.xausky.kch.zookeeper.quorum","localhost");
        columnFamily = props.getProperty("io.github.xausky.kch.hbase.column.family","d");

    }

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topic) {
        this.topics = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
