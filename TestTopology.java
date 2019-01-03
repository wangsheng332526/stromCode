
package cn.cnseller.synergy.data.handlex;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
  * TODO 请在此处添加注释
  * @author <a href="mailto:"wangsheng"@zjiec.com”>"wangsheng"</a>
  * @version 2017年5月22日  下午5:53:44  
  * @since 2.0
  */
public class TestTopology {
	public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testspout", new TestSpout());
        //spout的值会随机发送,如果bolt并行为3,spout发送5次的话,这3个bolt累计接收5次
//        builder.setBolt("testbolt", new TestBolt(),3).shuffleGrouping("testspout");
        
        //相同的值会发送给同一个bolt
//        builder.setBolt("testbolt", new TestBolt(),3).fieldsGrouping("testspout",new Fields("log"));
//        builder.setBolt("testbolt1", new TestBolt(),3).shuffleGrouping("testspout");
        //自定义分组
        builder.setBolt("testbolt", new TestBolt(),1).shuffleGrouping("testspout");
        builder.setBolt("testbolttwo", new TestBolt2(),1).shuffleGrouping("testbolt");
//        builder.setBolt("testbolt2", new TestBolt2(),3).customGrouping("testbolt", new ModuleGrouping());
//        builder.setBolt("testbolt2",new TestBolt2()).shuffleGrouping("testbolt");
        Config conf = new Config();
        conf.setDebug(false);
//        conf.setNumWorkers(3);  
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testtopology", conf, builder.createTopology());
        try {
			Thread.sleep(200000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        cluster.shutdown();
        System.out.println("........local cluster 关闭");
        
        //在一个真实的集群上运行自己的拓扑,当你使用StormSubmitter时，你就不能像使用LocalCluster时一样通过代码控制集群了。
//        try {
//			StormSubmitter.submitTopology("testtopology", conf, builder.createTopology());
//		} catch (AlreadyAliveException e) {
//			e.printStackTrace();
//		} catch (InvalidTopologyException e) {
//			e.printStackTrace();
//		}
	}
}
