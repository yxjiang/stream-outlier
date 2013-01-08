package edu.fiu.yxjiang.stream.bolt;

import java.util.Map;

import sysmon.common.metadata.MachineMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
/**
 * A generic <code>backtype.storm.topology.IRichBolt</code> implementation
 * for testing/debugging the Storm JMS Spout and example topologies.
 * <p/>
 * For debugging purposes, set the log level of the 
 * <code>backtype.storm.contrib.jms</code> package to DEBUG for debugging
 * output.
 * @author tgoetz
 *
 */
public class GenericBolt extends BaseRichBolt {
//	private static final Logger LOG = LoggerFactory.getLogger(GenericBolt.class);
	private static int boltCount = 0;
	private int boltID;
	
	private OutputCollector collector;
	private boolean autoAck = false;
	private Fields declaredFields;
	private String name;
	
	/**
	 * Constructs a new <code>GenericBolt</code> instance.
	 * 
	 * @param name The name of the bolt (used in DEBUG logging)
	 * @param autoAck Whether or not this bolt should automatically acknowledge received tuples.
	 */
	public GenericBolt(String name, boolean autoAck){
		this.name = name;
		this.autoAck = autoAck;
		this.boltID = ++boltCount;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String machineIP = input.getString(1);
		MachineMetadata machineMetadata = (MachineMetadata)input.getValue(1);
		System.out.printf("\n\nboltID: %d, [%s] CPU: %f\n\n\n\n\n", boltID, machineIP, machineMetadata.getCpu().getIdleTime());
//		System.out.println("\n\nReceived message:");
//		LOG.debug("[" + this.name + "] Received message: " + input);
		
//		// only emit if we have declared fields.
//		if(this.declaredFields != null){
//			LOG.debug("[" + this.name + "] emitting: " + input);
//			if(this.autoAnchor){
//				this.collector.emit(input, input.getValues());
//			} else{
//				this.collector.emit(input.getValues());
//			}
//		}
//		
//		if(this.autoAck){
//			LOG.debug("[" + this.name + "] ACKing tuple: " + input);
//			this.collector.ack(input);
//		}
		
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		if(this.declaredFields != null){
//			declarer.declare(this.declaredFields);
//		}
	}
	
	public boolean isAutoAck(){
		return this.autoAck;
	}
	
	public void setAutoAck(boolean autoAck){
		this.autoAck = autoAck;
	}

}
