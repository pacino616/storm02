package cn.py.trident;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class AgeCombineAggregator implements CombinerAggregator<Integer>{
	/**
	 * 此方法是组件的初始化方法，此方法会返回一个初始值 val1,
	 * val1会传给combine方法
	 */
	@Override
	public Integer zero() {
		return 0;
	}
	
	@Override
	public Integer combine(Integer val1, Integer val2) {
		int ageSum = val1+val2;
		return ageSum;
	}

	/**
	 * 此方法会接受上游发来的tuple
	 * 此方法会返回val2值，并且传给combine方法
	 */
	@Override
	public Integer init(TridentTuple tuple) {
		int age = tuple.getIntegerByField("age");
		return age;
	}


}
