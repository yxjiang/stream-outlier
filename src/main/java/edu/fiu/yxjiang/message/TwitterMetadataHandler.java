package edu.fiu.yxjiang.message;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import edu.fiu.yxjiang.stream.util.TwitterMetadata;

/**
 * Handle the twitter metadata.
 * @author Yexi Jiang (http://users.cs.fiu.edu/~yjian004)
 *
 */
public class TwitterMetadataHandler extends MessageHandler{

	private int offset = 0;
	
	@Override
	public void handleDeclare(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "id", "changeRatio"));				
	}

	@Override
	public Values toTuple(Message message) {
		if(message instanceof ObjectMessage) {
			ObjectMessage objectMessage = (ObjectMessage)message;
			try {
				TwitterMetadata metadata = (TwitterMetadata)objectMessage.getObject();
				String id = metadata.getTopic();
				long timestamp = offset++;
				return new Values(timestamp, id, metadata);
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

}
