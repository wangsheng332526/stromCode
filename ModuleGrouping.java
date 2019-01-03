
package cn.cnseller.synergy.data.handlex;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

/**
  *采用单词首字母字符的整数值与任务数的余数，决定接收元组的bolt。
  * @author <a href="mailto:"wangsheng"@zjiec.com”>"wangsheng"</a>
  * @version 2017年6月26日  下午5:05:12  
  * @since 2.0
  */
public class ModuleGrouping implements CustomStreamGrouping{

	 /**    */
	private static final long serialVersionUID = 8525293443662400497L;
	
	int numTasks =0;
	public static void main(String[] args) {
		String s = "x";
		System.out.println( (int)s.charAt(0));
		
	}
	
	/**
	 * @param paramInt
	 * @param paramList
	 * @return
	 * @see backtype.storm.grouping.CustomStreamGrouping#chooseTasks(int, java.util.List)
	 */
	@Override
	public List<Integer> chooseTasks(int paramInt, List<Object> values) {
		// TODO Auto-generated method stub
		List<Integer> boltIds = new ArrayList<Integer>();
		if(values.size()>0){
			String str = values.get(0).toString();
//			System.out.println(str+"....."+str.charAt(0)%numTasks);
			if(str.isEmpty()){
//				boltIds.add(1);
			}else{
				boltIds.add(str.charAt(0)%numTasks+1);
			}
		}
		return boltIds;
	}

	
	/**
	 * @param paramWorkerTopologyContext
	 * @param paramGlobalStreamId
	 * @param paramList
	 * @see backtype.storm.grouping.CustomStreamGrouping#prepare(backtype.storm.task.WorkerTopologyContext, backtype.storm.generated.GlobalStreamId, java.util.List)
	 */
	@Override
	public void prepare(WorkerTopologyContext paramWorkerTopologyContext, GlobalStreamId paramGlobalStreamId,
			List<Integer> paramList) {
		numTasks = paramList.size();
		System.out.println("任务数"+numTasks);
	}

}
