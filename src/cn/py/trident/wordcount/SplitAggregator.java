package cn.py.trident.wordcount;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitAggregator extends BaseAggregator<String>{

	@Override
	public void aggregate(String val, TridentTuple tuple, TridentCollector collector) {
		String line = tuple.getStringByField("line");
		String[] words = line.split(" ");
		for (String word : words) {
			collector.emit(new Values(word));
		}
	}

	@Override
	public void complete(String val, TridentCollector tuple) {
	}

	@Override
	public String init(Object arg0, TridentCollector arg1) {
		return null;
	}

}
