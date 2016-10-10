package hadoop_test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.coordination.CoordinatedBolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStorm {
	
	private static final Logger log = LoggerFactory.getLogger(TestStorm.class);
	private final static String[] info = {"hello", "world", "this", "is", "storm", "demo"};

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("SpoutDemo", new SpoutDemo());
		builder.setBolt("BoltDemo1", new BoltDemo(), 2).fieldsGrouping("SpoutDemo", new Fields("msg1"));//.shuffleGrouping("SpoutDemo");
//		builder.setBolt("BoltDemo2", new BoltDemo(), 2).fieldsGrouping("SpoutDemo", new Fields("msg2"));//.shuffleGrouping("SpoutDemo");
		
		Config conf = new Config();
		conf.setMaxTaskParallelism(2);
		
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
			String msg1 = info[random.nextInt(5)];
			String msg2 = info[random.nextInt(5)];
			
//			System.out.println("in spout:" + msg);
			collector.emit(new Values(msg1, msg2));
			
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("msg1", "msg2"));
		}

		@Override
		public void ack(Object msgId) {
			log.info("ack: " + msgId);
		}

		@Override
		public void fail(Object msgId) {
			log.info("fail: " + msgId);
		}
		
	}

	public static class BoltDemo extends BaseBasicBolt {
		
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String msg = tuple.getString(1);
			System.out.println(Thread.currentThread().toString() + ": in bolt: " + msg + " is processed!");
//			log.info(Thread.currentThread().toString() + ": " + msg);
			collector.emit(new Values(msg + ", this msg is processed!"));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("msg"));
		}
		
	}

}
