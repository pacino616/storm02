package cn.py.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Aggregator聚合器，输出的tuple可以有一个或多个key字段
 * 泛型定义的是init()方法的返回值类型，如果初始化方法不使用的话，泛型可以不加；
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
