package com.daxian.bigdata.storm.blot;

import com.daxian.bigdata.storm.Utils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * 拆分矩阵为倒排索引
 * @author qiandaxian
 */
public class MatrixSpiltBlot extends BaseRichBolt {

    private OutputCollector outputCollector;
    Logger logger = LoggerFactory.getLogger(MatrixSpiltBlot.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        int totle = 0;
        int x = tuple.getIntegerByField("x");
        Set<Integer> ySet = (Set)tuple.getValueByField("y");
        Iterator<Integer> it = ySet.iterator();
        while (it.hasNext()){
            int y = it.next();
            if(Utils.intersectionCheck(x,y,x+y)){
                totle++;
            }
        }
        logger.debug("筛选符合条件的数据{}条记录",totle);
        outputCollector.emit(new Values(x,totle));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("x","count"));
    }

}
