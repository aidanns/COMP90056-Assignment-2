package com.aidanns.streams.assignment.two.bolt;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import twitter4j.Status;
import twitter4j.internal.logging.Logger;

/**
 * Bolt that will record the top K words in statuses that it processes and will
 * output them to output/top_statuses.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
@SuppressWarnings("serial")
public class TopKWordsBolt extends BaseStatusBolt {

	/** Delay before first write in ms. */
	private long FILE_WRITE_DELAY = 0;

	/** Delay between writes in ms. */
	private long FILE_WRITE_PERIOD = 1000;

	/** File to write output to. */
	private String OUTPUT_FILE_NAME = "output/words.txt";

	/** Counter for Tweets processed. */
	private long _numberOfWordsProcessesd = 0;

	/** Time that the bolt was started. */
	private Date _startDate;

	/** Data structure implementing the TopK algorithm. */
	private StreamSummary<String> _topWords;
	
	/** Number of words we're interested in. */
	private int _numWords;
	
	/**
	 * Create a new TopKWordsBolt.
	 * @param numWords The number of words that we want to know about.
	 */
	public TopKWordsBolt(int numWords) {
		_numWords = numWords;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		
		 _topWords = new StreamSummary<String>(_numWords * 10);

		// Setup output writing at a fixed interval.
		Timer outputToFileTimer = new Timer();
		outputToFileTimer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (_startDate != null) {
					float numSecondsSinceStart = 
							(new Date().getTime() - _startDate.getTime()) / 1000f;
					long throughput = 
							(long) (_numberOfWordsProcessesd / numSecondsSinceStart);

					Writer writer = null;

					try {
						writer = new BufferedWriter(new OutputStreamWriter(
								new FileOutputStream(OUTPUT_FILE_NAME), "utf-8"));
						writer.write("Top Words Statistics:\n");
						writer.write("\n");
						writer.write("# Words processed: " 
								+ _numberOfWordsProcessesd + "\n");
						writer.write("Time elapsed:       " 
								+ numSecondsSinceStart + "\n");
						writer.write("Throughput:         " 
								+ throughput + " wps\n");
						
						writer.write("\n");
						for (Counter<String> counter : _topWords.topK(20)) {
							writer.write("Item: " + counter.getItem() + " Count: " 
									+ counter.getCount() + " Error: " 
									+ counter.getError() + "\n");
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
		}, FILE_WRITE_DELAY, FILE_WRITE_PERIOD);
	}

	@Override
	void processStatus(Status status) {
		if (_startDate == null) {
			 _startDate = new Date();
		}
		StringTokenizer tokenizer = new StringTokenizer(status.getText());
		while (tokenizer.hasMoreElements()) {
			_topWords.offer(tokenizer.nextToken());
			++_numberOfWordsProcessesd;
		}

	}

}
