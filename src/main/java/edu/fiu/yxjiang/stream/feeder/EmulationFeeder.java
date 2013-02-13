package edu.fiu.yxjiang.stream.feeder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import sysmon.util.IPUtil;
import edu.fiu.yxjiang.stream.util.TwitterMetadata;

/**
 * Feed the synthetic data to spout.
 * @author Yexi Jiang (http://users.cs.fiu.edu/~yjian004)
 *
 */
public class EmulationFeeder implements Runnable{
	
	private List<Map<String, Integer>> data;
	private Map<String, Map<Long, Integer>> topicData;
	private long minTimestamp = Long.MAX_VALUE;
	private long maxTimestamp = 0;

	//	broker
	private BrokerService broker;
	private String port = "34567";
	private String brokerAddress = "tcp://" + IPUtil.getFirstAvailableIP() + ":" + port;
	private String messageTopicName = "command";
	
	//	messager
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Topic topic;
	private MessageProducer producer;
	
	public EmulationFeeder(String filename) throws NumberFormatException, IOException {
		this.data = new ArrayList<Map<String, Integer>>();
		this.topicData = new HashMap<String, Map<Long, Integer>>();
		//	read data for each topic
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		while((line = br.readLine()) != null) {
			Map<Long, Integer> topicMap = new HashMap<Long, Integer>();
			String[] tokens = line.split("\t");
			if(tokens.length != 3) {
				continue;
			}
			String[] pairs = tokens[2].split(" ");
			for(String pair : pairs) {
				String[] kv = pair.split(":");
				long timestamp = Long.parseLong(kv[0]);
				if(timestamp < minTimestamp) {
					minTimestamp = timestamp;
				}
				if(timestamp > maxTimestamp) {
					maxTimestamp = timestamp;
				}
				int count = Integer.parseInt(kv[1]);
				topicMap.put(timestamp, count);
			}
			topicData.put(tokens[0], topicMap);
		}
		
		System.out.printf("Min timestamp: %d, max timestamp: %d.\n", minTimestamp, maxTimestamp);
		
		for(long t = minTimestamp; t <= maxTimestamp; ++t) {
			Map<String, Integer> occurTopic = new HashMap<String, Integer>();
			for(Map.Entry<String, Map<Long, Integer>> topicEntry : topicData.entrySet()) {
				String topicName = topicEntry.getKey();
				Map<Long, Integer> topicMap = topicEntry.getValue();
				Integer count = topicMap.get(t);
				occurTopic.put(topicName, count == null ? 0 : count);
			}
			data.add(occurTopic);
		}
		System.out.println("Length of emulation:" + data.size());
		
		initBroker();
		try {
			initMessagerSender();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	public String getBrokerAddress() {
		return brokerAddress;
	}
	
	private void initBroker() {
		broker = new BrokerService();
		broker.setBrokerName("emulationBroker");
		try {
			broker.setPersistent(false);
			broker.setUseJmx(false);
			broker.addConnector(brokerAddress);
			broker.start();
			System.out.println("Data feeder broker started...");
			System.out.println("Feed data to " + brokerAddress);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initMessagerSender() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory(brokerAddress);
		connection = connectionFactory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		topic = session.createTopic(messageTopicName);
		producer = session.createProducer(topic);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	}

	@Override
	public void run() {
		int interval = 100;
		//	send data to broker
		Map<String, Integer> sums = new HashMap<String, Integer>();
		for(int i = 0; i < data.size(); ++i) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Map<String, Integer> timestampData = data.get(i);
			System.out.printf("Time offset %d.\t\t", i);
			for(Map.Entry<String, Integer> entry : timestampData.entrySet()) {
				String topic = entry.getKey();
				Integer sum = sums.get(entry.getKey());
				if(sum == null) {
					sum = entry.getValue();
				}
				else {
					sum += entry.getValue();
				}
				sums.put(entry.getKey(), sum);
				float mvAvg = sum / (i + 1);
				int ratio = mvAvg == 0 ? entry.getValue() : (int)(entry.getValue() / mvAvg);
				System.out.print(ratio + "\t");
				try {
					TwitterMetadata metadata = new TwitterMetadata(topic, ratio);
					Message message = session.createObjectMessage(metadata);
					producer.send(message);
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println();
		}
	}
	
	public static void main(String[] args) throws Exception {
		String filename = "/home/yxjiang/dataset/twitter_political_post_processed";
		EmulationFeeder feeder = new EmulationFeeder(filename);
		Thread thread = new Thread(feeder);
		thread.start();
		thread.join();
	}
	
	
}
