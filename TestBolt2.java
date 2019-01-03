
package cn.cnseller.synergy.data.handlex;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
  * TODO 请在此处添加注释
  * @author <a href="mailto:"wangsheng"@zjiec.com”>"wangsheng"</a>
  * @version 2017年5月27日  上午11:07:43  
  * @since 2.0
  */
public class TestBolt2 extends BaseRichBolt{
	String name;
	int id;
	int ii;
	OutputCollector collector;
	/**
	 * @param paramTuple
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//如果传的是实体,用 input.getValue(0)
//		String str = input.getString(0);
//		System.out.println("....name:"+name+id+",value:"+str);
		System.out.println(".....第二个bolt执行了....");
//		try {
//			Thread.sleep(30000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		collector.ack(input);
//		collector.fail(input);
	}

	
	/**
	 * @param paramMap
	 * @param paramTopologyContext
	 * @param paramOutputCollector
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map paramMap, TopologyContext paramTopologyContext, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		name = paramTopologyContext.getThisComponentId();
		id = paramTopologyContext.getThisTaskId();
	}

	
	/**
	 * @param paramOutputFieldsDeclarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("word2"));
	}

}
