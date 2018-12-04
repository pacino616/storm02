package cn.py.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Aggregator�ۺ����������tuple������һ������key�ֶ�
 * ���Ͷ������init()�����ķ���ֵ���ͣ������ʼ��������ʹ�õĻ������Ϳ��Բ��ӣ�
 *
 */
public class AgeAggregator extends BaseAggregator<Integer>{
	
	private int ageSum = 0;
	
	@Override
	public Integer init(Object arg0, TridentCollector arg1) {
		return 0;
	}
	
	@Override
	public void aggregate(Integer val, TridentTuple tuple, TridentCollector collector) {
		int age = tuple.getIntegerByField("age");
		ageSum = ageSum + age;
		collector.emit(new Values("aa",ageSum));
		
	}

	@Override
	public void complete(Integer arg0, TridentCollector arg1) {
		
	}


}
