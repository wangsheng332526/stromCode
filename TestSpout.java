
package cn.cnseller.synergy.data.handlex;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.cnseller.synergy.manager.service.OperatorService;
import cn.cnseller.synergy.manager.service.SendPackService;

/**
 * TODO 请在此处添加注释
 * 
 * @author <a href="mailto:"wangsheng"@zjiec.com”>"wangsheng"</a>
 * @version 2017年5月22日 上午11:45:08
 * @since 2.0
 */
public class TestSpout extends BaseRichSpout {

//    private static final Logger LOGGER = LoggerFactory.getLogger(TestSpout.class);
	 
	static AtomicInteger sAtomicInteger = new AtomicInteger(0);
	static AtomicInteger pendNum = new AtomicInteger(0);
	private int sqnum;
	SpoutOutputCollector collector;
	SendPackService sendPackService;
	OperatorService operatorService;
	private String track;
	int ii=0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("beans.xml");
//		sendPackService = applicationContext.getBean(SendPackService.class);
//		operatorService = applicationContext.getBean(OperatorService.class);
//		System.out.println("...open...");
		this.collector = collector;
		
		//从context对象获取spout大小
		int spoutsSize = context.getComponentTasks(context.getThisComponentId()).size();
		//从这个spout得到任务id
	    int myIdx = context.getThisTaskIndex();
	    
		System.out.println("spoutsSize:"+spoutsSize+",taskId:"+myIdx);
		
	}

	@Override
	public void nextTuple() {
		System.out.println(".....spout开始执行...");
		this.collector.emit(new Values("o"),1);
		try {
			Thread.sleep(500000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		this.collector.emit(new Values("p"),2);
//		this.collector.emit(new Values("q"),3);
//		this.collector.emit(new Values("x"),4);	
//		this.collector.emit(new Values("y"),5);
//		this.collector.emit(new Values("z"),6);
//		this.collector.emit(new Values("x"),7);	
//		this.collector.emit(new Values("z"),10);
//		this.collector.emit(new Values("o"),8);
//		this.collector.emit(new Values("z"),9);
//		this.collector.emit(new Values("p"),11);
//		this.collector.emit(new Values("y"),12);
//		try {
//			Thread.sleep(500000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		while (true) {
//			int a = pendNum.incrementAndGet();
//		    LOGGER.info(String.format("spount %d,pendNum %d", sqnum, a));
//			this.collector.emit(new Values("xxxxx:" + a));
//
//			try {
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));

	}

	/**
	 * 启用 ack
	 * 机制，详情参考：https://github.com/alibaba/jstorm/wiki/Ack-%E6%9C%BA%E5%88%B6
	 * 
	 * @param msgId
	 */
	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
	}

	/**
	 * 消息处理失败后需要自己处理
	 * 
	 * @param msgId
	 */
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		ii++;
	    System.out.println("....ii:"+ii+",msgId:"+msgId);
	}
}
