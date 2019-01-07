package com.daxian.bigdata.storm.spout;

import com.daxian.bigdata.storm.producer.MatrixDataProducer;
import com.daxian.bigdata.storm.model.MatrixData;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * 生产实时数据
 * 内容为：矩阵数据 （x,y）
 * @author qiandaxian
 */
public class MatrixSpout extends BaseRichSpout {

    static Logger logger = LoggerFactory.getLogger(MatrixDataProducer.class);

    private SpoutOutputCollector spoutOutputCollector;

    private MatrixDataProducer matrixDataProducer;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        matrixDataProducer = new MatrixDataProducer(100000,999999);
        matrixDataProducer.start();
    }

    /**
     * 生产矩阵数据
     */
    @Override
    public void nextTuple() {

        Map<Integer,Set> data = new HashMap();

        int count = 0;

        while (!matrixDataProducer.isEmpty()){

            //5000条刷一次
            if(count>5000){
                break;
            }

            MatrixData matrixData = matrixDataProducer.poll();

            if(data.get(matrixData.getX())==null){
                Set ys = new HashSet();
                ys.add(matrixData.getY());
                data.put(matrixData.getX(),ys);
            }else {
                Set ys = data.get(matrixData.getX());
                ys.add(matrixData.getY());
                data.put(matrixData.getX(),ys);
            }
            count++;
        }

        data.keySet().forEach(x->{
            spoutOutputCollector.emit(new Values(x, data.get(x)));
        });

    }

    /**
     * 定义输入的数据名称
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("x","y"));
    }
}
