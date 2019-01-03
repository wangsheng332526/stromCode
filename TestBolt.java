
package cn.cnseller.synergy.data.handlex;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import cn.cnseller.synergy.manager.model.Operator;
import cn.cnseller.synergy.manager.service.OperatorService;

/**
  * TODO 请在此处添加注释
  * @author <a href="mailto:"wangsheng"@zjiec.com”>"wangsheng"</a>
  * @version 2017年5月27日  上午11:07:43  
  * @since 2.0
  */
public class TestBolt extends BaseRichBolt{
	String name;
	int id;
	OutputCollector collector;
	OperatorService operatorService;
	public static ApplicationContext applicationContext = new ClassPathXmlApplicationContext("beans.xml");
	
	/**
	 * @param paramTuple
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String str = input.getString(0);
//		System.out.println(name+id+"....bolt begin...."+str+"....");
		Operator o = new Operator();
		o.setId(2);
//		operatorService.getCache("891kjhksd213a");
		System.out.println("bolt执行了....");
		collector.emit(new Values(o,1));
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
//		applicationContext = new ClassPathXmlApplicationContext("beans.xml");
//		operatorService = applicationContext.getBean(OperatorService.class);
		
		
	}

	
	/**
	 * @param paramOutputFieldsDeclarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("word","id"));
	}

}
