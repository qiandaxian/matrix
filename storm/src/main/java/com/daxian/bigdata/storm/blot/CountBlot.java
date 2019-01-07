package com.daxian.bigdata.storm.blot;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CountBlot extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(CountBlot.class);
    Long result = 0L;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        new Thread(){
//
//            @Override
//            public void run() {
//                while (true) {
//                    logger.info("已获取到{}条匹配结果", count);
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }.start();
    }

    @Override
    public void execute(Tuple tuple) {
        int x = tuple.getIntegerByField("x");
        int count = tuple.getIntegerByField("count");
        result = result + count;
        logger.info("x:{}已获取到{}条匹配结果,总计{}条结果",x,count,result);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
