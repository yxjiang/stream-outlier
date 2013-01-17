package edu.fiu.yxjiang.message;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;


import sysmon.common.metadata.MachineMetadata;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ComputerMetadataHandler extends MessageHandler{

	@Override
	public void handleDeclare(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "id", "metadataJson"));		
	}

	@Override
	public Values toTuple(Message message) {
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
