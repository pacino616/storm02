package cn.py.trident;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class AgeReduceAggregator implements ReducerAggregator<Integer>{
	
	/**
	 * �˷����൱��������ĳ�ʼ�������������curr��������reducer����
	 */
	@Override
	public Integer init() {
		
		return 0;
	}

	@Override
	public Integer reduce(Integer curr, TridentTuple tuple) {
		int age = tuple.getIntegerByField("age");
		int ageSum = curr+age;
		return ageSum;
	}

}
