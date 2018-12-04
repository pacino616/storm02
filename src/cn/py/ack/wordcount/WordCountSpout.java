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
		//storm at least once(����һ������)����Դ�ڷ���tupleʱ��Ҫ����һ��ȫ��Ψһ��tuple id
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
		//���tuple����ɹ������߿��ܻ���ɶ����
		map.remove(msgId);
	}
	
	/**
	 * �����ε��������failʱ�������˷���
	 * ������Ҫ����ʧ��tuple��id,���·���һ��
	 */
	@Override
	public void fail(Object msgId) {
		collector.emit(map.get(msgId), msgId);
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		//��ʼ��
		map = new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
