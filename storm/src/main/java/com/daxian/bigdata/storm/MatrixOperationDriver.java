package com.daxian.bigdata.storm;


import com.daxian.bigdata.storm.blot.CountBlot;
import com.daxian.bigdata.storm.blot.MatrixSpiltBlot;
import com.daxian.bigdata.storm.spout.MatrixSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * 矩阵运算驱动类
 * @author qiandaxian
 */
public class MatrixOperationDriver {
    static Logger logger = LoggerFactory.getLogger(MatrixOperationDriver.class);

    public static void main(String[] args) {


        logger.info("程序开始执行...");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("MatrixSpout",new MatrixSpout(),1);
        builder.setBolt("MatrixSpiltBlot",new MatrixSpiltBlot(),12).shuffleGrouping("MatrixSpout");
        builder.setBolt("CounterBolt",new CountBlot(),1).shuffleGrouping("MatrixSpiltBlot");

        Config config = new Config();
        config.setNumWorkers(2);

        //本地模式运行
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("Mx1",new Config(),builder.createTopology());


        //集群模式运行
        try {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
