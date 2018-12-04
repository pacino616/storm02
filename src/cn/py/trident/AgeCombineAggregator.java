package cn.py.trident;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class AgeCombineAggregator implements CombinerAggregator<Integer>{
	/**
	 * �˷���������ĳ�ʼ���������˷����᷵��һ����ʼֵ val1,
	 * val1�ᴫ��combine����
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
	 * �˷�����������η�����tuple
	 * �˷����᷵��val2ֵ�����Ҵ���combine����
	 */
	@Override
	public Integer init(TridentTuple tuple) {
		int age = tuple.getIntegerByField("age");
		return age;
	}


}
