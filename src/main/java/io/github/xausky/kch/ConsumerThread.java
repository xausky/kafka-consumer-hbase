package io.github.xausky.kch;

import io.github.xausky.kch.config.Config;
import io.github.xausky.kch.config.TopicConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by xausky on 10/26/16.
 */
public class ConsumerThread extends Thread {
    private static final Logger logger = Logger.getLogger(ConsumerThread.class.getName());

    private Config config;
    private TopicConfig topicConfig;
    private KafkaConsumer<String,String> consumer;
    private Connection connection;
    private Configuration conf;
    private Boolean running = false;


    public ConsumerThread(Config config, TopicConfig topicConfig) throws IOException {
        try {
            this.config = config;
            this.topicConfig = topicConfig;
            Properties props = new Properties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG,config.getGroupId());
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,config.getBootstrapServer());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringDeserializer.class);
            consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Collections.singletonList(topicConfig.getTopic()));
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", config.getZookeeper());
            conf.set("hbase.zookeeper.property.clientPort", String.valueOf(config.getPort()));
            connection = ConnectionFactory.createConnection(conf);
            running = true;
        }catch (IOException e){
            if (consumer!=null) {
                consumer.close();
            }
            if(connection!=null){
                connection.close();
            }
            throw e;
        }
    }
    @Override
    public void run() {
        Thread.currentThread().setName("ConsumerThread-"+topicConfig.getTopic());
        Admin admin = null;
        Table table = null;
        try {
            TableName tableName = null;
            byte[] columnFamily = Bytes.toBytes(config.getColumnFamily());
            byte[] valueColumn = Bytes.toBytes("value");
            admin = connection.getAdmin();
            tableName = TableName.valueOf(topicConfig.getTable());
            if(!admin.tableExists(tableName)){
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor("d"));
                admin.createTable(tableDescriptor);
            }
            table = connection.getTable(tableName);
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                logger.info(String.format("poll count:"+records.count()));
                List<Put> puts = new LinkedList<Put>();
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    if(key!=null && value!=null) {
                        Put put = new Put(Bytes.toBytes(key));
                        put.addColumn(columnFamily, valueColumn, Bytes.toBytes(value));
                        puts.add(put);
                    }
                }
                table.put(puts);
                logger.info(String.format("put count:"+puts.size()));
            }
        }catch (IOException e){
            running = false;
        }finally {
            if (consumer!=null) {
                consumer.close();
            }

            if(table!=null){
                try {
                    table.close();
                }catch (Exception e){}
            }
            if(admin!=null){
                try {
                    admin.close();
                }catch (Exception e){}
            }
            if(connection!=null){
                try {
                    connection.close();
                }catch (Exception e){}
            }

        }
    }

    public Boolean getRunning() {
        return running;
    }

    public void setRunning(Boolean running) {
        this.running = running;
    }
}
