package edu.fiu.yxjiang.message;

import javax.jms.Message;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

/**
 * MessageHandler is an abstract class that defines the actions should be done to handle a specific type of message.
 * @author yexijiang
 *
 */
public abstract class MessageHandler {
	
	/**
	 * Define the output fields.
	 * @param declarer
	 */
	public abstract void handleDeclare(OutputFieldsDeclarer declarer);
	
	/**
	 * Translate the message to values in Storm.
	 * @param message
	 * @return
	 */
	public abstract Values toTuple(Message message);
	
}
