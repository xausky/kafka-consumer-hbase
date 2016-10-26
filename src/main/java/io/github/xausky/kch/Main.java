package io.github.xausky.kch;

import io.github.xausky.kch.config.Config;
import io.github.xausky.kch.config.TopicConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by xausky on 10/26/16.
 */
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());
    public static void main(String[] args){
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("conf/consumer.properties"));
        }catch (IOException e){
            logger.warning("Config file conf/consumer.properties open fail:"+e.getMessage());
        }
        Config config = new Config(props);
        final Map<String,List<ConsumerThread>> consumersMap = new TreeMap<String,List<ConsumerThread>>();
        for(String topic:config.getTopics()){
            TopicConfig topicConfig = new TopicConfig(props,topic);
            List<ConsumerThread> consumers = new ArrayList<ConsumerThread>(topicConfig.getThreadCount());
            for(int i=0;i<topicConfig.getThreadCount();i++){
                try {
                    consumers.add(new ConsumerThread(config, topicConfig));
                }catch (IOException e){
                    logger.warning("Connect kafka or hbase fail:"+e.getMessage());
                }
            }
            consumersMap.put(topic,consumers);
        }
        for(Map.Entry<String,List<ConsumerThread>> entry:consumersMap.entrySet()){
            for(ConsumerThread consumer:entry.getValue()){
                consumer.start();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                for(Map.Entry<String,List<ConsumerThread>> entry:consumersMap.entrySet()){
                    for(ConsumerThread consumer:entry.getValue()){
                        consumer.setRunning(false);
                    }
                }
            }
        });

        for(Map.Entry<String,List<ConsumerThread>> entry:consumersMap.entrySet()){
            for(ConsumerThread consumer:entry.getValue()){
                try {
                    consumer.join();
                }catch (Exception e){}
            }
        }
    }
}
