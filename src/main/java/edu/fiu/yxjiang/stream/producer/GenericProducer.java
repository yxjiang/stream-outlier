package edu.fiu.yxjiang.stream.producer;

import javax.jms.JMSException;
import javax.jms.Message;

import edu.fiu.yxjiang.message.MessageHandler;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

public class GenericProducer<T extends MessageHandler> implements JmsTupleProducer {
	
	private T messageHandler;
	
	public GenericProducer(T messageHandler) {
		this.messageHandler = messageHandler;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		messageHandler.handleDeclare(declarer);
	}

	@Override
	public Values toTuple(Message message) throws JMSException {
		return messageHandler.toTuple(message);
	}

}
