package com.hzcominfo.ccetl.adapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataStruct {
    
    final private static ReentrantReadWriteLock fileIndexLocker =new ReentrantReadWriteLock();      //文件顺序取号锁
    final private static SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMddHHmmss");
    private static int fileIndex=0;
    private String flowName;
    private String fileName;        //对应文件名字
    AdapterDataOut adapterDataOut;
    void setFlowName(String flowName0)
    {
        flowName = flowName0;
    }
    String getFlowName()
    {
        return flowName;
    }
    void setFileName(String fileName0)
    {
        fileName = fileName0;
    }
    String getFileName()
    {
        if(fileName == null){
            fileName = createFileName(flowName);
        }
        return fileName;
    }
    
    private static int getFileIndex(){
        int index=0;
        try{
            fileIndexLocker.writeLock().lock();
            if(fileIndex>=999999)
                fileIndex=0;
            index = fileIndex++;
        }finally{
            fileIndexLocker.writeLock().unlock();
        }
        return index;
    }
    
    private static String createFileName(String flowName){
        return flowName+"_"+dateformat.format(new Date())+"_"+String.format("%06d", getFileIndex())+".data";
    }
}