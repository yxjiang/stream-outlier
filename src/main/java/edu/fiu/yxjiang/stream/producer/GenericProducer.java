package edu.fiu.yxjiang.stream.producer;

import javax.jms.JMSException;
import javax.jms.Message;

import edu.fiu.yxjiang.message.MessageHandler;
import edu.fiu.yxjiang.message.MessageHandlerFactory;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

public class GenericProducer implements JmsTupleProducer {
	
	private MessageHandler messageHandler;
	
	public GenericProducer(String messageHandlerName) {
		this.messageHandler = MessageHandlerFactory.getMessageHandler(messageHandlerName);
		try{
			if(this.messageHandler == null) {
				throw new Exception("No message handler match the name " + messageHandlerName);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
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
