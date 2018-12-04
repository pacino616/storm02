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
		//����Trident����ṩ������Դ��������������	Tuple
		//1�Σ�����tuple��key�ֶΣ�2�Σ�����С��3�Σ�����tuple��ֵ
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("name","age"),4,
				new Values("tom",23),
				new Values("rose",22),
				new Values("jary",28),
				new Values("jj",21));
		//����ѭ����������
//		spout.setCycle(true);
		
		//��ȡTrident��ܵ����˹�����
		TridentTopology topology = new TridentTopology();
		//������Դ
//		topology.newStream("spout", spout)
//		//��������������ÿ��Ը��ݹ������������˳���ص�tuple�������з���
//		.each(new Fields("name","age"), new NameFilter())
//		//Function��������Ի��������tuple��׷���µ��ֶβ����䵽����
//		.each(new Fields("name","age"),new GenderFunction(),new Fields("gender"))
//		.each(new Fields("name","age","gender"), new PringFilter());
		
		topology.newStream("spout", spout).
				//�������кϲ�(�ۺ�)
				partitionAggregate(new Fields("name","age"),
				new AgeAggregator(),
				new Fields("info","ageSum"))
				.each(new Fields("info","ageSum"), new PringFilter());
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("infoTopology", conf, topology.build());
	}
}
