package cn.py.ack.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordPrintBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple input) {
		try {
			String word = input.getStringByField("word");
			Integer count = input.getIntegerByField("count");
			System.out.println(word+":"+count);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
