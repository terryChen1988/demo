package hadoop_test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.CoordinatedBolt;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStormAck {
	
	private static final Logger log = LoggerFactory.getLogger(TestStormAck.class);
	
	private final static String[] info = {"hello", "world", "this", "is", "storm", "demo"};
	
	private final static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();

	public static void main(String[] args) {
		
		queue.add("hello");
		queue.add("world");
		queue.add("this");
		queue.add("is");
		queue.add("storm");
		queue.add("demo");
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("SpoutDemo", new SpoutDemo(), 1);
		builder.setSpout("SpoutDemo1", new SpoutDemo(), 1);
		builder.setBolt("BoltDemo", new BoltDemo(), 1).fieldsGrouping("SpoutDemo", new Fields("msg1")).fieldsGrouping("SpoutDemo1", new Fields("msg1"));//.shuffleGrouping("SpoutDemo");
		
		Config conf = new Config();
		conf.setMaxTaskParallelism(2);
		conf.setMessageTimeoutSecs(5);
		conf.setNumAckers(2);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormDemo", conf, builder.createTopology());

//		try {
//			Thread.sleep(2000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		cluster.shutdown();
	}
	
	public static class SpoutDemo extends BaseRichSpout {

		private SpoutOutputCollector collector;
		
		Random random = new Random();

		@Override
		public void nextTuple() {
			
			String msg1 = queue.poll();
			
			collector.emit(new Values(msg1), msg1);
			
			try {
				Thread.sleep(10*60*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("msg1"));
		}

		@Override
		public void ack(Object msgId) {
			log.info("ack: " + msgId);
		}

		@Override
		public void fail(Object msgId) {
			log.info("fail: " + msgId) ;
			queue.add(msgId.toString());
		}
		
	}
	
	public static class BoltDemo implements IRichBolt {
		
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input) {
			
			
//			try {
//				Thread.sleep(40 * 1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}

			log.info("!!!!!" + input.toString());
			
			if (null != input.getString(0)) {
				log.info(Thread.currentThread().toString() + ": msg is " + input.getString(0));
				
				if (input.getString(0).startsWith("h") || input.getString(0).startsWith("s")) {
					collector.fail(input);
				} else {
					collector.ack(input);
				}
			}
			
//			if (input.getString(0).startsWith("h") || input.getString(0).startsWith("s")) {
//				collector.fail(input);
//			} else {
//				collector.ack(input);
//			}
			
		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("msg"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

}
