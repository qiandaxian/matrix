package com.daxian.bigdata.storm.producer;

import com.daxian.bigdata.storm.model.MatrixData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author qiandaxian
 * 生成需要运算的数据放入队列，队列满了线程等待
 */
public class MatrixDataProducer extends Thread {

    private int start;
    private int end;
    private BlockingQueue<MatrixData> blockingQueue = new LinkedBlockingQueue(1000000);

    public MatrixDataProducer(int start, int end) {
        this.start = start;
        this.end = end;
    }

    static Logger logger = LoggerFactory.getLogger(MatrixDataProducer.class);


    @Override
    public void run() {

        int start = this.start;
        int end = this.end;

        //x start->end
        for (int i = start; i < end; i++) {
            //TODO 可排尾数为0的数字

            logger.info("生成矩阵数据...---> x:{},y:({}-{})", i, i, end);
            //y x->end
            for (int j = i; j < end; j++) {
                //重试
                int repeatNumber = 0;
                while (!blockingQueue.offer(new MatrixData(i, j))) {
                    try {
                        repeatNumber++;
                        logger.info("x:{},y:{}队列已满，待消费，线程等待{}毫秒",i ,j ,repeatNumber * 500);
                        Thread.sleep(500 * repeatNumber);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public MatrixData poll(){
        return blockingQueue.poll();
    }

    public boolean isEmpty(){
        return blockingQueue.isEmpty();
    }

}
