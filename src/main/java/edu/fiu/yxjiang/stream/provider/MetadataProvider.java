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
 * Provider is used by spout to receive the data from message queue.
 * @author yexijiang
 *
 */
@SuppressWarnings("serial")
public class MetadataProvider implements JmsProvider{

	private static int providerCount = 0;
	
	private int providerIdx;
	private ConnectionFactory connectionFactory;
	private Topic destination;
	
	
	public MetadataProvider(List<String> gatherBrokerAddressList) {
		this.providerIdx = providerCount++;
		this.connectionFactory = new ActiveMQConnectionFactory(gatherBrokerAddressList.get(providerIdx));
		try {
			Connection connection = this.connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.destination = session.createTopic("command");
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
