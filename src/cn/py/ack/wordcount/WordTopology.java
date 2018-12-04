package cn.py.ack.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordTopology {
	
	public static void main(String[] args) throws Exception{
		Config conf = new Config();
		
		WordCountSpout countSpout = new WordCountSpout();
		WordSplitBolt splitBolt = new WordSplitBolt();
		WordCountBolt countBolt = new WordCountBolt();
		WordPrintBolt printBolt = new WordPrintBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("countSpout", countSpout);
		builder.setBolt("splitBolt", splitBolt,2).setNumTasks(4).shuffleGrouping("countSpout");
		builder.setBolt("countBolt", countBolt,2).fieldsGrouping("splitBolt",new Fields("word"));
		builder.setBolt("printBolt", printBolt).globalGrouping("countBolt");
		
		StormTopology topology = builder.createTopology();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("countSpout", conf, topology);
		
//		StormSubmitter cluster = new StormSubmitter();
//		cluster.submitTopology("WordCountTopology", conf, topology);
	}
}
