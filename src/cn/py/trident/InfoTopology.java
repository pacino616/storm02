package cn.py.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;

public class InfoTopology {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Config conf = new Config();
		//利用Trident框架提供的数据源，可以批量发送	Tuple
		//1参：发送tuple的key字段，2参：批大小；3参：发送tuple的值
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("name","age"),4,
				new Values("tom",23),
				new Values("rose",22),
				new Values("jary",28),
				new Values("jj",21));
		//设置循环发送数据
//		spout.setCycle(true);
		
		//获取Trident框架的拓扑构建者
		TridentTopology topology = new TridentTopology();
		//绑定数据源
//		topology.newStream("spout", spout)
//		//过滤器组件的作用可以根据过滤条件，过滤出相关的tuple，不进行发送
//		.each(new Fields("name","age"), new NameFilter())
//		//Function组件，可以基于输入的tuple，追加新的字段并发射到下游
//		.each(new Fields("name","age"),new GenderFunction(),new Fields("gender"))
//		.each(new Fields("name","age","gender"), new PringFilter());
		
		topology.newStream("spout", spout).
				//按批进行合并(聚合)
				partitionAggregate(new Fields("name","age"),
				new AgeAggregator(),
				new Fields("info","ageSum"))
				.each(new Fields("info","ageSum"), new PringFilter());
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("infoTopology", conf, topology.build());
	}
}
