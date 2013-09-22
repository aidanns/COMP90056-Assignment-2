package com.aidanns.streams.assignment.two.topology;

import java.util.Properties;

import twitter4j.internal.logging.Logger;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.aidanns.streams.assignment.two.bolt.PrintMessageBolt;
import com.aidanns.streams.assignment.two.bolt.StatusThroughputRecorderBolt;
import com.aidanns.streams.assignment.two.bolt.TopKWordsBolt;
import com.aidanns.streams.assignment.two.spout.TwitterFileSpout;
import com.aidanns.streams.assignment.two.spout.TwitterStreamSpout;

/**
 * Main class to run the storm job, reading from a local file. Runs in a local cluster.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
public class AssignmentTwoFromFile {
	
	/** The file that contains the filename to read tweets from. */
	private static String INPUT_FILE_PROPERTIES_FILE_NAME = "input.properties";

	/** Logger, lazily loaded by {@link #getLogger()}. */
	private static Logger _logger;
	
	/**
	 * Lazily load a logger.
	 * @return Logger to be used in this class.
	 */
	private static Logger getLogger() {
		if (_logger == null) {
			_logger = Logger.getLogger(TwitterStreamSpout.class);
		}
		return _logger;
	}
	
	/**
	 * Run the job.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {
		
		// Load the name of the input file from disk.
		Properties inputProperties = new Properties();
		try {
			inputProperties.load(AssignmentTwoFromFile.class.getClassLoader()
					.getResourceAsStream(INPUT_FILE_PROPERTIES_FILE_NAME));
		} catch (Throwable e) {
			getLogger().error("Failed to open file specifying the name of the"
					+ " input file: " + INPUT_FILE_PROPERTIES_FILE_NAME);
			System.exit(1);
		}
		String inputFileName = inputProperties.getProperty("input.tweets.filename");

		TopologyBuilder builder = new TopologyBuilder();

		// Setup the spouts.
		builder.setSpout("twitter-sample-spout", new TwitterFileSpout(inputFileName), 1);

		// Setup the bolts.
		builder.setBolt("print-message", new PrintMessageBolt(), 1)
				.shuffleGrouping("twitter-sample-spout");
		builder.setBolt("throughput-recorder", new StatusThroughputRecorderBolt(), 1)
				.shuffleGrouping("twitter-sample-spout");
		builder.setBolt("top-20-words", new TopKWordsBolt(20, true), 1)
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
			Logger.getLogger(AssignmentTwoFromStream.class).error("Interrupted while"
					+ " waiting for local cluster to complete processing.");
			e.printStackTrace();
		}
		cluster.shutdown();
		System.exit(1);
	}
	
}
