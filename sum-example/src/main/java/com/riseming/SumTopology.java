package com.riseming;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SumTopology {

    public static class DataSourceSpout extends BaseRichSpout{
        private Logger logger = LoggerFactory.getLogger(DataSourceSpout.class);
        //定义发射器
        private SpoutOutputCollector collector = null;
        int number = 0;
        /**
         * @param conf 配置参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            logger.info("DataSourceSpout init");
            this.collector = collector;
        }

        /**
         * 生产数据(生产：从队列中获取数据)，死循环会一直执行
         */
        @Override
        public void nextTuple() {
            this.collector.emit(new Values(++number));
            logger.info("Spout-Send:{}",number);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number"));
        }
    }

    /**
     * 接收数据求和
     */
    public static class SumBolt extends BaseRichBolt {
        private Logger logger = LoggerFactory.getLogger(SumBolt.class);
        int sum = 0;
        /**
         * @param stormConf
         * @param context
         * @param collector 需要再向后发布
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        @Override
        public void execute(Tuple input) {
            //通过字段名称获取参数
            Integer value = input.getIntegerByField("number");
            sum +=value;
            logger.info("Bolt:sum = {}",sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
    public static void main(String[] args){
        //本地模式
        LocalCluster cluster = new LocalCluster();
        /**
         * 构建一个拓扑图，Storm中的任何一个作业都是通过Topology提交的
         * Topology需要指定Spout和bolt的执行顺序
         */
        TopologyBuilder builder  = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        //指定SumBolt 从DataSourceSpout中接收数据
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");
        cluster.submitTopology("SumTopology", new Config(), builder.createTopology());
    }
}
