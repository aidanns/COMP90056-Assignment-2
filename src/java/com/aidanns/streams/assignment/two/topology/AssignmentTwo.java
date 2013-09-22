package com.aidanns.streams.assignment.two.topology;

import java.util.Map;

import twitter4j.Status;
import twitter4j.internal.logging.Logger;

import com.aidanns.streams.assignment.two.bolt.StatusThroughputRecorderBolt;
import com.aidanns.streams.assignment.two.bolt.TopKWordsBolt;
import com.aidanns.streams.assignment.two.spout.TwitterStreamSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Main class to run the storm job. Runs in a local cluster.
 */
public class AssignmentTwo {
	
	/**
	 * PrintMessageBolt accepts a message and prints it to the logger as a
	 * message with INFO precedence.
	 */
	private static class PrintMessageBolt extends BaseBasicBolt {
		
		private static final long serialVersionUID = -2810876355107441445L;
		
		private Logger _logger;
		
		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
				TopologyContext context) {
			_logger = Logger.getLogger(PrintMessageBolt.class);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			Status status = (Status) tuple.getValue(0);
			_logger.info(status.getText());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// No output.
			return;
		}
	}

	/**
	 * Run the job.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		// Setup the spouts.
		builder.setSpout("twitter-sample-spout", new TwitterStreamSpout(), 1);

		// Setup the bolts.
		builder.setBolt("print-message", new PrintMessageBolt(), 1)
				.shuffleGrouping("twitter-sample-spout");
		builder.setBolt("throughput-recorder", new StatusThroughputRecorderBolt(), 1)
				.shuffleGrouping("twitter-sample-spout");
		builder.setBolt("top-20-words", new TopKWordsBolt(20), 1)
				.shuffleGrouping("twitter-sample-spout");

		// Start the job.
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("assignment-2", conf, builder.createTopology());

		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			Logger.getLogger(AssignmentTwo.class).error("Interrupted while"
					+ " waiting for local cluster to complete processing.");
			e.printStackTrace();
		}
		cluster.shutdown();
		System.exit(1);
	}

}
