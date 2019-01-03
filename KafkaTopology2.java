
package cn.cnseller.synergy.data.handlex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import kafka.api.OffsetRequest;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
  * 导入storm-kafka.jar, 从kafka中读取信息
  * @author <a href="mailto:"wangsheng"@zjiec.com”>"wangsheng"</a>
  * @version 2017年5月24日  下午5:13:22  
  * @since 2.0
  */
public class KafkaTopology2 {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopology2.class);
	
	public static void main(String[] args) {
		  //zookeeper的服务器地址
		String zks = "127.0.0.1:2181";
        //消息的topic
        String topic = "test";
        //strom在zookeeper上的根
        String zkRoot = "/storm";
        String id = "test";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        
    	//如果将forceFromStart(旧版本是ignoreZkOffsets）设置为true，则每次拓扑重新启动时，都会从开头读取消息。
        //如果为false，则：第一次启动，从开头读取，之后的重启均是从offset中读取。
        spoutConf.ignoreZkOffsets = false;
        spoutConf.zkServers = Arrays.asList("127.0.0.1".split(","));
        spoutConf.zkPort = 2181;
//        spoutConf.startOffsetTime = OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf)); 
        builder.setBolt("save-message", new SaveMessageBolt(),2).shuffleGrouping("kafka-reader");
        Config conf = new Config();
        conf.setDebug(false);
        //设置任务线程数
        conf.setMaxTaskParallelism(2);
        conf.setMessageTimeoutSecs(60);
        
        //本地启动方式
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("kafkaTopology", conf, builder.createTopology());
//        try {
//			Thread.sleep(200000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//        cluster.shutdown();
        
        //提交集群方式
        try {
			StormSubmitter.submitTopology("kafkaTopology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
    }	
}
