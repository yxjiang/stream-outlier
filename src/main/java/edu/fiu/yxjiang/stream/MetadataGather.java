package edu.fiu.yxjiang.stream;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import sysmon.common.metadata.MachineMetadata;
import sysmon.subscriber.Subscriber;
import sysmon.util.GlobalParameters;
import sysmon.util.IPUtil;

/**
 * Gather all the meta-data collected by the specified collectors.
 * 
 * @author Yexi Jiang (http://users.cs.fiu.edu/~yjian004)
 * 
 */
public class MetadataGather {

	private Connection connection;
	private Subscriber sub;

	public MetadataGather(String gatherIP, List<String> inputBrokerAddressList) {
		try {
			init(gatherIP, inputBrokerAddressList);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void init(String gatherIP, List<String> inputBrokerAddressList) throws JMSException {
		sub = new Subscriber(inputBrokerAddressList);
		String brokerAddress = "tcp://" + gatherIP + ":" + GlobalParameters.SUBSCRIBE_COMMAND_PORT;
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerAddress);
		connection = connectionFactory.createConnection();
		connection.start();

	}

	public void close() {
		try {
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
		sub.stop();
	}

	/*
	 * public static void main(String[] args) { String gatherIP =
	 * IPUtil.getFirstAvailableIP(); List<String> list = new ArrayList<String>();
	 * list.add("tcp://131.94.128.171:" +
	 * GlobalParameters.COLLECTOR_COMMAND_PORT); // list.add("tcp://" +
	 * IPUtil.getFirstAvailableIP() + ":" +
	 * GlobalParameters.COLLECTOR_COMMAND_PORT);
	 * 
	 * MetadataGather gather = new MetadataGather(gatherIP, list);
	 * 
	 * }
	 */
}
