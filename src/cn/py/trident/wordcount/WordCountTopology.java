package cn.py.trident.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import cn.py.trident.PringFilter;
import storm.trident.TridentTopology;

public class WordCountTopology {
	public static void main(String[] args) throws Exception{
		Config conf = new Config();
		WordCountSpout spout = new WordCountSpout();
		
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("spout", spout)
		//�������
		.shuffle()
		//�з����
		.partitionAggregate(new Fields("line"), new SplitAggregator(),new Fields("word"))
		.parallelismHint(3)
		.partitionBy(new Fields("word"))
		//����Ƶ�����
		.partitionAggregate(new Fields("word"), new WordCoundAggregator(), new Fields("word","count"))
		.parallelismHint(2);
//		.global()
//		//��ӡ���
//		.each(new Fields("word","count"), new PringFilter());
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("topology", conf, topology.build());
	}
}
