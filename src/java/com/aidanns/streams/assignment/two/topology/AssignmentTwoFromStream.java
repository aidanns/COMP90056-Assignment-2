package com.aidanns.streams.assignment.two.topology;

import twitter4j.internal.logging.Logger;

import com.aidanns.streams.assignment.two.bolt.StatusThroughputRecorderBolt;
import com.aidanns.streams.assignment.two.bolt.TopKWordsBolt;
import com.aidanns.streams.assignment.two.bolt.PrintMessageBolt;
import com.aidanns.streams.assignment.two.spout.TwitterStreamSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Main class to run the storm job. Runs in a local cluster.
 */
public class AssignmentTwoFromStream {
	
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		// Setup the spouts.
		builder.setSpout("twitter-sample-spout", new TwitterStreamSpout(), 1);

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
