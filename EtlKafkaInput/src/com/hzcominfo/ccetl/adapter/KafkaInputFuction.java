package com.hzcominfo.ccetl.adapter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

class KafkaInputFuction{
    
    public final static Logger logger = KafkaInput.logger;
    public final static ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
    public AdapterFlow flow;
    public final static HashMap<String, KafkaData> threadMap = new HashMap<>();
    public List<String> fieldList = new LinkedList<>();
    private Properties props;
    public static HashMap<String, List<DataStatus>> dataOutMap = new HashMap<>();
    
    public KafkaInputFuction(Properties props){
        this.props = props;
    }
    
    public int prepare(
            AdapterTableInfo paramTableInfo,
            List < AdapterFieldOut >  paramFieldOutList,
            List < AdapterCond > paramCondList,
            List < AdapterTableUse > paramTableList,
            List < AdapterJoin> paramJoinList             
            ) 
    {
        KafkaInputFuction.locker.writeLock().lock();
        try {
            fieldList.clear();
            for (AdapterFieldOut afo : paramFieldOutList) {
                fieldList.add(afo.fieldName);
            }
            KafkaData localDealThread = threadMap.get(flow.flowName);
            if (localDealThread == null) {
                localDealThread = new KafkaData(paramTableInfo.tableName, props);
                localDealThread.flow = flow;
                localDealThread.fieldList = fieldList;
                threadMap.put(flow.flowName, localDealThread);
                localDealThread.run();
                logger.info("flow " + flow.flowName + " TxtDealThread create");
            }
        } finally {
            KafkaInputFuction.locker.writeLock().unlock();
        }
        return 0;
    }

    public int getData(String flowName, AdapterDataOut paramValue) {
        int rc = 0;
        AdapterDataOut dataOut = null;
        DataStatus dss = null;
        KafkaInputFuction.locker.writeLock().lock();
        try {
            List<DataStatus> tfsList = dataOutMap.get(flow.flowName);
            if (tfsList != null && !tfsList.isEmpty()) {
                dss = tfsList.remove(0);
                dss.dealedFlag = 1;
                dataOut = dss.ado;
            }
        } finally {
            KafkaInputFuction.locker.writeLock().unlock();
        }
        if (dataOut != null) {
            paramValue.keyList.addAll(dataOut.keyList);
            paramValue.operType = dataOut.operType;
            paramValue.operTypeList.addAll(dataOut.operTypeList);
            paramValue.valueList.addAll(dataOut.valueList);
            if (paramValue.valueList != null) {
                rc = paramValue.valueList.size();
            }
            //dataOut = null;

            //删除文件,插入日志
            //KafkaData.InsertFileLog(dss);
            //delFile(dss);
        }
        return rc;
    }
    
    //数据存到文件
    
    //删除数据文件
    
    //拉出来文件数据
    
    //比较出未处理的文件数据
}