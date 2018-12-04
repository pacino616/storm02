package cn.py.ack.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplitBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple input) {
		try {
			String line = input.getStringByField("line");
			String[] words = line.split(" ");
			for (String word : words) {
				//上下游锚定机制
				collector.emit(input, new Values(word));
			}
			//向上游反馈ack
			collector.ack(input);
		} catch (Exception e) {
			//向上游反馈fail
			collector.fail(input);
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
