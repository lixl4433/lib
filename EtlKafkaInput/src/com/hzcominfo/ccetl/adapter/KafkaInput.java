/*
 * 
 */
package com.hzcominfo.ccetl.adapter;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;


public class KafkaInput implements IAdapterInput {
    
//  全局变量    
    public final static Logger logger=Logger.getLogger("HttpInput");
    
    public KafkaInputFuction kafkaInputFuc;
    public AdapterDataSource actionDataSource;
    public AdapterFlow flow;
    int port = 0;
    /**
     * 适配器初始化
     * 
     * @param paramDriverName   驱动名字，例如 jdbc:oracle:thin<br\>
     * @param paramDataSource    数据源信息
     * @param paramActionDataSource 执行etl时的数据源信息
     * @param paramDataSourceList 所有的数据源
     * @param paramFlow 抽取流程
     * @return &gt;0 成功 <br/>
     * &lt;0 失败<br/>
     * 0    已经执行过初始化
     */
    @Override
    public int init(String paramDriverName,
            AdapterDataSource paramDataSource,
            AdapterDataSource paramActionDataSource,
            List < AdapterDataSource > paramDataSourceList,
            AdapterFlow paramFlow)
    {
        actionDataSource=paramActionDataSource;
        flow=paramFlow;
        port = paramActionDataSource.port;//将参数port赋值过来
        logger.info("flow "+flow.flowName+" HttpInput init");
        //加载链接配置
        Properties props = null;
        if(null == kafkaInputFuc) {
            kafkaInputFuc = new KafkaInputFuction(props);
        }
        return 1;            
    }
    
    
/**
 * 关闭适配器
 * @return &gt;0 成功 <br/>
     * &lt;0 失败<br/>
     * 0    已经关闭
 */    
    @Override
    public int uninit()
    {
        logger.info("flow "+flow.flowName+" HttpInput uninit"); 
        //JettyServer.stop(port);//关闭一个
        return 1;
    }
    
/**
 * 执行全量抽取的准备工作
 * @param paramTableInfo 要抽取的主表
 * @param paramFieldOutList 输出的字段清单
 * @param paramCondList sql条件
 * @param paramTableList 使用的表信息
 * @return 
 */
    @Override
    public int prepareFullAction(
            AdapterTableInfo paramTableInfo,
            List < AdapterFieldOut >  paramFieldOutList,
            List < AdapterCond > paramCondList,
            List < AdapterTableUse > paramTableList,
            List < AdapterJoin> paramJoinList             
            )
    {
        int rc=1;
        return rc;
    }
    
    
/**
 * 执行快速增量抽取的准备工作
 * @param paramTableInfo 要抽取的主表
 * @param paramFieldOutList 输出的字段清单
 * @param paramCondList sql条件
 * @param paramTableList 使用的表信息
 * @return 
 */
    @Override
    public int prepareRapidIncrAction(
            AdapterTableInfo paramTableInfo,
            List < AdapterFieldOut >  paramFieldOutList,
            List < AdapterCond > paramCondList,
            List < AdapterTableUse > paramTableList,
            List < AdapterJoin> paramJoinList             
            )
    {
        //加载数据配置
        //
        int rc=0;
        return rc;
    }
    

/**
 * 执行增量抽取的准备工作
 * @param paramTableInfo 要抽取的主表
 * @param paramFieldOutList 输出的字段清单
 * @param paramCondList sql条件
 * @param paramTableList 使用的表信息
 * @return 
 */
    @Override
    public int prepareIncrAction(
            AdapterTableInfo paramTableInfo,
            List < AdapterFieldOut >  paramFieldOutList,
            List < AdapterCond > paramCondList,
            List < AdapterTableUse > paramTableList,
            List < AdapterJoin> paramJoinList             
            )
    {
        kafkaInputFuc.prepare(paramTableInfo, paramFieldOutList, paramCondList, paramTableList, paramJoinList);
        int rc=0;
        return rc;
    }
    
    /**
     * 获取给定数量的全量抽取数据
     * 全量抽取程序将会持续调用此接口来获取记录。
     * @param paramValue    存放数据的地方
     * @param paramMaxRow 本次获取的最大记录数
     * @return 记录数，当返回的记录数&lt;要求的最大记录数时，表示全量抽取完毕
     */
    @Override
    public int getFullData(AdapterDataOut paramValue,
            int paramMaxRow)
    {
		int rc = kafkaInputFuc.getData(flow.flowName,paramValue);
        return rc;
    }
    
    /**
     * 获取给定批量的快速增量数据，此接口会频繁调用
     * @param paramValue 存放数据 
     * @param paramMaxRow   最多获取的记录数。
     * @param paramLastValue 上次获取的key值，用于按字段的值实现增量的情况。
     * @return 返回记录数
     */
    
    @Override
    public int getRapidIncrData(AdapterDataOut paramValue,
            int paramMaxRow,Object paramLastValue)
    {
		int rc = kafkaInputFuc.getData(flow.flowName,paramValue);
        return rc;
    }
    

    /**
     * 获取给定的批量增量数据，此接口会频繁调用
     * @param paramValue 存放数据 
     * @param paramMaxRow   最多获取的记录数。
     * @return 返回记录数
     */
    @Override
    public int getIncrData(AdapterDataOut paramValue,
            int paramMaxRow)
    {
        return 0;
    }
    
    /**
     * 停止Etl，当etl主程序停止一个etl抽取过程的时候
     */
    @Override
    public void stopAction()
    {
    }
    
/**
 * 更新增量数据状态
 * @param paramIncrStatusList 状态记录列表
 * @param paramValueList 记录列表，和状态记录列表一一对应。
 * @return 成功处理的记录数
 */
    @Override
    public int updateIncrStatus(List < AdapterIncrStatus> paramIncrStatusList,
    List < Map < String,Object > > paramValueList)
    {
        return 0;
    }
    
    
/**
 * 检查主表中数据是否存在
 * @param paramKey key字段的值
 * @return &gt;0 存在  0 不存在 &lt;0系统错误
 */
    @Override
    public int checkKeyExist(Object paramKey)
    {
        return 1;
    }
    
/**
 * 获取对应key的输出记录值
 * @param paramValue    输出的记录
 * @param paramKey  key值
 * @return 返回的记录数
 */
    @Override
    public int getKeyData(AdapterDataOut paramValue,
            Object paramKey)
    {
        return 1;
    }

    
    public static void SysSleep(long paramTime)
    {
        try
        {
            Thread.sleep(paramTime);
        }
        catch(Exception e)
        {
            logger.error("SysSleep", e);
        }
    }
}
