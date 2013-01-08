package edu.fiu.yxjiang.stream.topology;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Session;

import sysmon.util.GlobalParameters;
import sysmon.util.IPUtil;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import edu.fiu.yxjiang.stream.MetadataGather;
import edu.fiu.yxjiang.stream.bolt.DataInstancesScoreBolt;
import edu.fiu.yxjiang.stream.bolt.StreamScoreBolt;
import edu.fiu.yxjiang.stream.producer.MetadataProducer;
import edu.fiu.yxjiang.stream.provider.MetadataProvider;

public class StreamOutlierTopology {
	public static final String INTERMEDIATE_BOLT = "INTERMEDIATE_BOLT";
	public static final String FINAL_BOLT = "FINAL_BOLT";
	public static final String JMS_TOPIC_BOLT = "JMS_TOPIC_BOLT";
	public static final String JMS_TOPIC_SPOUT = "JMS_TOPIC_SPOUT";
	public static final String ANOTHER_BOLT = "ANOTHER_BOLT";

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		// start up metadata gather
		String gatherIP = IPUtil.getFirstAvailableIP();
		
		String collectorIP = "131.94.128.171";
		List<String> list = new ArrayList<String>();
		list.add("tcp://" + collectorIP + ":" + GlobalParameters.COLLECTOR_COMMAND_PORT);
//		list.add("tcp://192.168.0.108:" + GlobalParameters.COLLECTOR_COMMAND_PORT);
		
		MetadataGather gather = new MetadataGather(gatherIP, list);
		System.out.println("Gather broker setted!!");

		String gatherBrokerAddress = "tcp://" + gatherIP + ":"
				+ GlobalParameters.SUBSCRIBE_COMMAND_PORT;

		// JMS Queue Provider
		JmsProvider jmsTopicProvider = new MetadataProvider(gatherBrokerAddress);

		// JMS Producer
		JmsTupleProducer producer = new MetadataProducer();

		// JMS Queue Spout
		JmsSpout queueSpout = new JmsSpout();
		queueSpout.setJmsProvider(jmsTopicProvider);
		queueSpout.setJmsTupleProducer(producer);
		queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
		queueSpout.setDistributed(true); // allow multiple instances

		TopologyBuilder builder = new TopologyBuilder();

		// spout with 1 parallel instances
		builder.setSpout(JMS_TOPIC_SPOUT, queueSpout, 1);

		// intermediate bolt, subscribes to jms spout, anchors on tuples, and
		// auto-acks
//		GenericBolt genericBolt = new GenericBolt(INTERMEDIATE_BOLT, true);
//		builder.setBolt(INTERMEDIATE_BOLT, genericBolt, 2).fieldsGrouping(JMS_TOPIC_SPOUT, new Fields("machineIP"));

		DataInstancesScoreBolt dataInstScoreBolt = new DataInstancesScoreBolt();
		builder.setBolt("DataInstancesScoreBolt", dataInstScoreBolt, 1).shuffleGrouping(JMS_TOPIC_SPOUT);
		
		StreamScoreBolt streamScoreBolt = new StreamScoreBolt();
		builder.setBolt("StreamScoreBolt", streamScoreBolt, 1).fieldsGrouping("DataInstancesScoreBolt", new Fields("entityID"));
		
		double lambda = 0.5;	//	exponential decay parameter
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("lambda", lambda);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-jms-example", conf, builder.createTopology());
		Utils.sleep(1000000);
		cluster.killTopology("storm-jms-example");
		cluster.shutdown();
		gather.close();
		System.exit(1);
	}

}
