package edu.fiu.yxjiang.stream.topology;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;

import sysmon.common.metadata.MachineMetadata;
import sysmon.util.GlobalParameters;
import sysmon.util.IPUtil;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.fiu.yxjiang.stream.MetadataGather;
import edu.fiu.yxjiang.stream.bolt.ObservationScoreBolt;
import edu.fiu.yxjiang.stream.bolt.SlidingWindowStreamAnomalyScoreBolt;
import edu.fiu.yxjiang.stream.bolt.TopKAlertTriggerBolt;
import edu.fiu.yxjiang.stream.producer.GenericProducer;
import edu.fiu.yxjiang.stream.provider.GenericInputProvider;
import edu.fiu.yxjiang.stream.provider.GenericOutputProvider;
import edu.fiu.yxjiang.stream.util.Bean;

/**
 * LOF + Sliding window + Top-K
 * @author yexijiang
 *
 */
public class LOF2 {
	
	public static final String DATA_TYPE = "computerMetaData";
	
	public static final String JMS_INPUT_JMS_TOPIC = "command";
	public static final String JMS_SPOUT = "JMS SPOUT";
	public static final String DATA_INSTANCE_SCORER = "DATA INSTANCE SCORER";
	public static final String STREAM_SCORER = "STREAM SCORER";
	public static final String ALERT_TRIGGER = "ALERT TRIGGER";
	public static final String ALERT_JMS_BOLT = "ALERT JMS BOLT";
	
	public static final String ALERT_TOPIC = "alert";
	
	public static final int ALERT_PORT = 33333;
	public static final String alertBrokerAddress = "tcp://" + IPUtil.getFirstAvailableIP() + ":" + ALERT_PORT;

	public static BrokerService broker;
	
	public static void initAlertBroker() {
		broker = new BrokerService();
		broker.setBrokerName("commandBroker");
		try {
			broker.setPersistent(false);
			broker.setUseJmx(false);
			broker.addConnector(alertBrokerAddress);
			broker.start();
			System.out.println("Alert broker started...");
			System.out.println("Send message to " + alertBrokerAddress);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
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
		
		initAlertBroker();

		//	the gather that aggregate all the message 
		String gatherBrokerAddress = "tcp://" + gatherIP + ":" + GlobalParameters.SUBSCRIBE_COMMAND_PORT;
		
		List<String> gatherBrokerAddressList = new ArrayList<String>();
		gatherBrokerAddressList.add(gatherBrokerAddress);

		// JMS Topic Provider
		JmsProvider jmsTopicProvider = new GenericInputProvider(gatherBrokerAddressList, JMS_INPUT_JMS_TOPIC);
		
		// JMS Producer
		JmsTupleProducer producer = new GenericProducer(DATA_TYPE);

		// JMS Topic Spout
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
		
		SlidingWindowStreamAnomalyScoreBolt streamScoreBolt = new SlidingWindowStreamAnomalyScoreBolt();
		builder.setBolt(STREAM_SCORER, streamScoreBolt, 1).fieldsGrouping(DATA_INSTANCE_SCORER, new Fields("id"));
		
		//	always report the top-K streams as abnormal
		TopKAlertTriggerBolt alertTriggerBolt = new TopKAlertTriggerBolt();
		builder.setBolt(ALERT_TRIGGER, alertTriggerBolt, 1).shuffleGrouping(STREAM_SCORER);
		
		JmsProvider jmsOutputTopicProvider = new GenericOutputProvider(alertBrokerAddress, ALERT_TOPIC);
		JmsBolt jmsBolt = new JmsBolt();
		jmsBolt.setJmsProvider(jmsOutputTopicProvider);

		jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
			@Override
			public Message toMessage(Session session, Tuple input) throws JMSException {
				Bean bean = new Bean();
				bean.timestamp = input.getLong(2);
				bean.id = input.getString(0);
				bean.score = input.getDouble(1);
				bean.isAbnormal = input.getBoolean(3);
				bean.observation = (MachineMetadata)input.getValue(4);
				
				ObjectMessage om = session.createObjectMessage(bean);
				return om;
			}
		});

		builder.setBolt(ALERT_JMS_BOLT, jmsBolt).shuffleGrouping(ALERT_TRIGGER);
		
		double lambda = 0.017;	//	exponential decay parameter
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("lambda", lambda);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-system-anomaly-detection", conf, builder.createTopology());
//		while(true) {
//			Utils.sleep(1000000);
//		}
//		cluster.killTopology("storm-jms-example");
//		cluster.shutdown();
//		gather.close();
//		System.exit(1);
	}

}

