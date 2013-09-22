package com.aidanns.streams.assignment.two.bolt;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.aidanns.streams.assignment.two.datastructure.SpaceSaving;
import com.aidanns.streams.assignment.two.datastructure.SpaceSaving.Counter;

import twitter4j.Status;
import twitter4j.User;
import twitter4j.internal.logging.Logger;

/**
 * Bolt that will record the top K users posting statusses that it processes
 * and will output them to output/users.txt.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
@SuppressWarnings("serial")
public class TopKUsersBolt extends BaseStatusBolt {
	
	/** Delay before first write in ms. */
	private long FILE_WRITE_DELAY = 0;

	/** Delay between writes in ms. */
	private long FILE_WRITE_PERIOD = 1000;

	/** File to write output to. */
	private String OUTPUT_FILE_NAME = "output/users";
	
	/** Counter for Tweets processed. */
	private long _numberOfTweetsProcessed = 0;

	/** Time that the bolt was started. */
	private Date _startDate;
	
	/** Number of users that we want to record. */
	private int _numUsers;
	
	/** Data structure implementing the TopK algorithm. */
	private SpaceSaving<User> _topUsers;
	
	/**
	 * TimerTask that will write the output from this bolt out to a file.
	 */
	private class OutputTopKUsersTask extends TimerTask {
		
		private boolean _shouldAppendMinutesToFileName;
		
		public OutputTopKUsersTask(boolean shouldAppendMinutesToFileName) {
			_shouldAppendMinutesToFileName = shouldAppendMinutesToFileName;
		}

		@Override
		public void run() {
			if (_startDate != null) {
				float numSecondsSinceStart = 
						(new Date().getTime() - _startDate.getTime()) / 1000f;
				long throughput = 
						(long) (_numberOfTweetsProcessed / numSecondsSinceStart);

				Writer writer = null;

				try {
					if (_shouldAppendMinutesToFileName) {
						writer = new BufferedWriter(new OutputStreamWriter(
								new FileOutputStream(OUTPUT_FILE_NAME + "_" + Math.round(numSecondsSinceStart / 60) + ".txt"), "utf-8"));
					} else {
						writer = new BufferedWriter(new OutputStreamWriter(
								new FileOutputStream(OUTPUT_FILE_NAME + ".txt"), "utf-8"));
					}
					
					writer.write("Top Users Statistics:\n");
					writer.write("\n");
					writer.write("# Tweet processed: " 
							+ _numberOfTweetsProcessed + "\n");
					writer.write("Time elapsed:       " 
							+ numSecondsSinceStart + "\n");
					writer.write("Throughput:         " 
							+ throughput + " tps\n");
					
					writer.write("\n");
					synchronized (_topUsers) {
						for (Counter<User> counter : _topUsers.topK(_numUsers)) {
							writer.write("Item: " + counter.getItem().getName() + " Count: " 
									+ counter.getCount() + " Error: " 
									+ counter.getError() + "\n");
						}
					}
				} catch (IOException ex) {
					Logger.getLogger(StatusThroughputRecorderBolt.class).error(
							"Error while writing words statistics.");
				} finally {
					try {
						writer.close();
					} catch (Exception ex) {}
				}
			}
		}
		
	}
	
	/**
	 * Create a new TopKUsersBolt.
	 * @param numUsers The number of top posting users that we want to output
	 *     data for.
	 */
	public TopKUsersBolt(int numUsers) {
		_numUsers = numUsers;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		
		 _topUsers = new SpaceSaving<User>(_numUsers * 1000);

		// Setup output writing at a fixed interval.
		Timer outputToFileTimer = new Timer();
		outputToFileTimer.scheduleAtFixedRate(new OutputTopKUsersTask(false), FILE_WRITE_DELAY, FILE_WRITE_PERIOD);
		// This question required output after every 5 minutes.
		outputToFileTimer.scheduleAtFixedRate(new OutputTopKUsersTask(true), 0, 1000 * 60 * 5);
	}

	@Override
	void processStatus(Status status) {
		if (_startDate == null) {
			 _startDate = new Date();
		}
		synchronized (_topUsers) {
			_topUsers.offer(status.getUser());
		}
		++_numberOfTweetsProcessed;
	}

}
