package edu.fiu.yxjiang.stream.producer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.tuple.Tuple;

/**
 * The AlertMessageProducer produces the alerts by jms.
 * @author yexijiang
 *
 */
public class AlertMessageProducer implements JmsMessageProducer{

	@Override
	public Message toMessage(Session session, Tuple tuple) throws JMSException {
		String alertStr = "Alert on " + tuple.getString(0);
		TextMessage alertMessage = session.createTextMessage(alertStr);
		return alertMessage;
	}

}
