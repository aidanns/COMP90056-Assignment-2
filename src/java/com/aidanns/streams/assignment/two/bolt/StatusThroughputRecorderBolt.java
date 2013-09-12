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
import twitter4j.Status;
import twitter4j.internal.logging.Logger;

@SuppressWarnings("serial")
public class StatusThroughputRecorderBolt extends BaseStatusBolt {
	
	/** Delay before first write in ms. */
	private long FILE_WRITE_DELAY = 0;
	
	/** Delay between writes in ms. */
	private long FILE_WRITE_PERIOD = 1000;
	
	/** File to write output to. */
	private String OUTPUT_FILE_NAME = "output/throughput.txt";
	
	/** Counter for Tweets processed. */
	private long _numberOfTweetsProcessed = 0;
	
	/** Time that the bolt was started. */
	private Date _startDate;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, 
			TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		
		// Setup output writing at a fixed interval.
		Timer outputToFileTimer = new Timer();
		outputToFileTimer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				if (_startDate != null) {
					float numSecondsSinceStart = 
							(new Date().getTime() - _startDate.getTime()) / 1000f;
					long throughput = 
							(long) (_numberOfTweetsProcessed / numSecondsSinceStart);
					
					Writer writer = null;
	
					try {
					    writer = new BufferedWriter(new OutputStreamWriter(
					    		new FileOutputStream(OUTPUT_FILE_NAME), "utf-8"));
					    writer.write("Throughput Statistics:\n");
					    writer.write("\n");
					    writer.write("# Tweets processed: " 
					    		+ _numberOfTweetsProcessed + "\n");
					    writer.write("Time elapsed:       " 
					    		+ numSecondsSinceStart + "\n");
					    writer.write("Throughput:         " 
					    		+ throughput + " tps\n");
					} catch (IOException ex) {
						Logger.getLogger(StatusThroughputRecorderBolt.class).error(
								"Error while writing throughput statistics.");
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
		++_numberOfTweetsProcessed;
	}

}
