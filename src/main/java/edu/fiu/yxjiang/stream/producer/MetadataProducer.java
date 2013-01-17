package edu.fiu.yxjiang.stream.producer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import sysmon.common.metadata.MachineMetadata;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * The jms Producer used by JmsSpout, it declares which data would be received by the bolts.
 * @author yexijiang
 *
 */
@SuppressWarnings("serial")
public class MetadataProducer implements JmsTupleProducer{

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "machineIP", "metadataJson"));
	}

	@Override
	/**
	 * Translate the message to Value in storm.
	 */
	public Values toTuple(Message message) throws JMSException {
		if(message instanceof ObjectMessage) {
			ObjectMessage objectMessage = (ObjectMessage)message;
			try {
				MachineMetadata machineMetadata = (MachineMetadata)objectMessage.getObject();
				String machineIP = machineMetadata.getMachineIP();
				long timestamp = machineMetadata.getTimestamp();
				return new Values(timestamp, machineIP, machineMetadata);
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
}
