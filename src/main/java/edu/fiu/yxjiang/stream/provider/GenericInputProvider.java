package edu.fiu.yxjiang.stream.provider;

import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import backtype.storm.contrib.jms.JmsProvider;

/**
 * GenericProvider is in charge of connect to the message brokers.
 * It will feed message to the spout.
 * @author yexijiang
 *
 */
public class GenericInputProvider implements JmsProvider{

	private static int providerCount = 0;	//	total number of providers
	
	private int providerIdx;
	private ConnectionFactory connectionFactory;
	private Topic destination;
	
	public GenericInputProvider(List<String> gatherBrokerAddressList, String topicName) {
		this.providerIdx = providerCount++;
		this.connectionFactory = new ActiveMQConnectionFactory(gatherBrokerAddressList.get(this.providerIdx));
		try {
			Connection connection = this.connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.destination = session.createTopic(topicName);
		} catch (JMSException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public ConnectionFactory connectionFactory() throws Exception {
		return this.connectionFactory;
	}

	@Override
	public Destination destination() throws Exception {
		return this.destination;
	}
}
