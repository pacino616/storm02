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
	//����trident��ܵ������Ķ���
	private TridentOperationContext context;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		//��ʼ��
		this.context = context;
	}
	
	@Override
	public void aggregate(String val, TridentTuple tuple, TridentCollector collector) {
		String word = tuple.getStringByField("word");
		//��֤partitionBy�����Ƿ�ɹ�
		System.err.println("������ţ�"+context.getPartitionIndex()+",word:"+word);
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
