package com.hzcominfo.ccetl.adapter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaData implements Runnable {
    
    public AdapterFlow flow = null;
    public List<String> fieldList;//字段名   = new LinkedList<>()
    
    private final int MESSAGE_NO = 1000;
    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private final String topic;
    private Map<AdapterFlow, Long> offset_map;

    public KafkaData(String topicName, Properties props) {
       /* props.put("bootstrap.servers", initConfigs.getBootstrap_servers());
        props.put("group.id", initConfigs.getGroup_id());
        props.put("enable.auto.commit", initConfigs.getEnable_auto_commit());
        props.put("auto.commit.interval.ms", initConfigs.getAuto_commit_interval_ms());
        props.put("session.timeout.ms", initConfigs.getSession_timeout_ms());
        props.put("auto.offset.reset", initConfigs.getAuto_offset_reset());
        props.put("key.deserializer", initConfigs.getKey_deserializer());
        props.put("value.deserializer", initConfigs.getValue_deserializer());//StringDeserializer.class.getName()*/
        this.consumer = new KafkaConsumer<String, String>(props);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        int messageNo = 0;
        try {
            for (;;) {
                msgList = consumer.poll(1000);
                if(null!=msgList&&msgList.count()>0){
                    for (ConsumerRecord<String, String> record : msgList) {
                        record.offset();
/*                        if(messageNo % MESSAGE_NO == 0){
                            System.out.println(messageNo+"=======receive: key = " + record.key() + ", value = " + record.value()+" offset==="+record.offset());
                        }*/
                        //当消费了1000条就退出
                        if(messageNo % MESSAGE_NO==0){
                            long offset_now = record.offset();
                            offset_map.put(flow, offset_now);
                            break;
                        }
                        messageNo++;
                    }
                }else{  
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
        pushData(msgList);
    } 
    
    int pushData(ConsumerRecords<String, String> msgList){
        //推入存储
        DataStatus dss = new DataStatus();
        dss.ado = null;
        dss.record = 1;

        KafkaInputFuction.locker.writeLock().lock();
        try {
            List<DataStatus> dssList = KafkaInputFuction.dataOutMap.get(flow.flowName);
            if (dssList != null) {
                dssList.add(dss);
            } else {
                dssList = new LinkedList<>();
                dssList.add(dss);
                KafkaInputFuction.dataOutMap.put(flow.flowName, dssList);
                //放入本地文件
                //............
            }

            dss.dealedFlag = 1;
            dss.fileContent = null;
        } finally {
            KafkaInputFuction.locker.writeLock().unlock();
        }
        return 0;
    }
    
    int offset(long offset) {
        if(null == offset_map) {
            offset_map = new HashMap<AdapterFlow, Long>();
        }
        offset_map.put(flow, offset);
        return 0;
    }
}
