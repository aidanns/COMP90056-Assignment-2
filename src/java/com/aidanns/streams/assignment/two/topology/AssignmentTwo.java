package com.aidanns.streams.assignment.two.topology;

import twitter4j.internal.logging.Logger;

import com.aidanns.streams.assignment.two.bolt.PrintMessageBolt;
import com.aidanns.streams.assignment.two.bolt.StatusThroughputRecorderBolt;
import com.aidanns.streams.assignment.two.bolt.TopKWordsBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;

/**
 * Simple wrapper for the logic that starts the application. Needed to permit
 * simple editing of the application while maintaining the ability to run from
 * multiple spouts.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
public class AssignmentTwo {
	
	/**
	 * Run the application logic, draing input tweets from a specified spout.
	 * @param spout The spout to hook up to the topology.
	 */
	public static void runApplicationWithSpout(IRichSpout spout) {
		TopologyBuilder builder = new TopologyBuilder();

		// Setup the spouts.
		builder.setSpout("twitter-spout", spout, 1);

		// Setup the bolts.
		builder.setBolt("print-message", new PrintMessageBolt(), 1)
				.shuffleGrouping("twitter-spout");
		builder.setBolt("throughput-recorder", new StatusThroughputRecorderBolt(), 1)
				.shuffleGrouping("twitter-spout");
		builder.setBolt("top-20-words", new TopKWordsBolt(20, true), 1)
				.shuffleGrouping("twitter-spout");

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
