package edu.fiu.yxjiang.stream.provider;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import backtype.storm.contrib.jms.JmsProvider;

public class AlertProvider implements JmsProvider{
	
	private String brokerAddress;
	private ConnectionFactory connectionFactory;
	private Topic destination;
	
	public AlertProvider(String brokerAddress, String topicName) {
		this.brokerAddress = brokerAddress;
		this.connectionFactory = new ActiveMQConnectionFactory(brokerAddress);
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
