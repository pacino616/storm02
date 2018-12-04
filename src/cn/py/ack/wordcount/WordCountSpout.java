package cn.py.ack.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCountSpout extends BaseRichSpout{
	
	private SpoutOutputCollector collector;
	String[] lines = new String[]{"hello world","hello storm","hello 1811"};
	int index = 0;
	private Map<UUID,Values> map;
	
	@Override
	public void nextTuple() {
		String line = lines[index];
		index++;
		UUID msgId = UUID.randomUUID();
		map.put(msgId, new Values(line));
		//storm at least once(至少一次语义)数据源在发送tuple时，要设置一个全局唯一的tuple id
		collector.emit(new Values(line),msgId);
		if(index == lines.length){
			index = 0;
		}
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void ack(Object msgId) {
		//如果tuple处理成功，否者可能会造成堆溢出
		map.remove(msgId);
	}
	
	/**
	 * 当下游的组件反馈fail时，会进入此方法
	 * 我们需要根据失败tuple的id,重新发射一次
	 */
	@Override
	public void fail(Object msgId) {
		collector.emit(map.get(msgId), msgId);
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		//初始化
		map = new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
