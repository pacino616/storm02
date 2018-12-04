package cn.py.trident.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class WordCoundAggregator extends BaseAggregator<String>{

	private Map<String,Integer> map = new HashMap<>();
	//创建trident框架的上下文对象
	private TridentOperationContext context;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		//初始化
		this.context = context;
	}
	
	@Override
	public void aggregate(String val, TridentTuple tuple, TridentCollector collector) {
		String word = tuple.getStringByField("word");
		//验证partitionBy分区是否成功
		System.err.println("分区编号："+context.getPartitionIndex()+",word:"+word);
		if(map.containsKey(word)){
			map.put(word, map.get(word)+1);
		}else{
			map.put(word, 1);
		}
		collector.emit(new Values(word,map.get(word)));
	}

	@Override
	public void complete(String val, TridentCollector tuple) {
		
	}

	@Override
	public String init(Object arg0, TridentCollector arg1) {
		return null;
	}

}
