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
import backtype.storm.utils.Utils;
import edu.fiu.yxjiang.stream.MetadataGather;
import edu.fiu.yxjiang.stream.bolt.ObservationScoreBolt;
import edu.fiu.yxjiang.stream.producer.GenericProducer;
import edu.fiu.yxjiang.stream.provider.GenericProvider;

public class StreamAnomalyTopology {
	
	public static final String DATA_TYPE = "computerMetaData";
	
	public static final String JMS_INPUT_JMS_TOPIC = "command";
	public static final String JMS_SPOUT = "JMS SPOUT";
	public static final String DATA_INSTANCE_SCORER = "DATA INSTANCE SCORER";
	public static final String STREAM_SCORER = "STREAM SCORER";
	public static final String ALERT_TRIGGER = "ALERT TRIGGER";

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

		//	the gather that aggregate all the message 
		String gatherBrokerAddress = "tcp://" + gatherIP + ":" + GlobalParameters.SUBSCRIBE_COMMAND_PORT;
		
		List<String> gatherBrokerAddressList = new ArrayList<String>();
		gatherBrokerAddressList.add(gatherBrokerAddress);

		// JMS Queue Provider
		JmsProvider jmsTopicProvider = new GenericProvider(gatherBrokerAddressList, JMS_INPUT_JMS_TOPIC);
//		JmsProvider jmsTopicProvider = new MetadataProvider(gatherBrokerAddressList);
		
		// JMS Producer
		JmsTupleProducer producer = new GenericProducer(DATA_TYPE);
//		JmsTupleProducer producer = new MetadataProducer();

		// JMS Queue Spout
		JmsSpout queueSpout = new JmsSpout();
		queueSpout.setJmsProvider(jmsTopicProvider);
		queueSpout.setJmsTupleProducer(producer);
		queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
		queueSpout.setDistributed(true); // allow multiple instances

		TopologyBuilder builder = new TopologyBuilder();

		// 	spout with 1 parallel instances
		builder.setSpout(JMS_SPOUT, queueSpout, 1);

		//	link data instance scorer to spout
		ObservationScoreBolt dataInstScoreBolt = new ObservationScoreBolt(DATA_TYPE);
		builder.setBolt(DATA_INSTANCE_SCORER, dataInstScoreBolt, 1).shuffleGrouping(JMS_SPOUT);
		
//		StreamAnomalyScoreBolt streamScoreBolt = new StreamAnomalyScoreBolt();
//		builder.setBolt("StreamAnomalyScoreBolt", streamScoreBolt, 1).fieldsGrouping("DataInstancesScoreBolt", new Fields("entityID"));
		
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
