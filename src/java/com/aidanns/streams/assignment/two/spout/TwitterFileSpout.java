package com.aidanns.streams.assignment.two.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.log4j.Logger;

import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class TwitterFileSpout extends BaseRichSpout {
	
	/** Logger, lazily loaded by {@link #getLogger()}. */
	private Logger _logger;
	
	/**
	 * Lazily load a logger.
	 * @return Logger to be used in this class.
	 */
	private Logger getLogger() {
		if (_logger == null) {
			_logger = Logger.getLogger(TwitterStreamSpout.class);
		}
		return _logger;
	}
	
	/** Output collector for this storm spout. */
	private SpoutOutputCollector _collector;
	
	private String _fileName;
	
	private BufferedReader _tweetFileReader;
	
	/** Number of lines of input skipped due to parsing errors. */
	private int _linesSkipped = 0;

	/**
	 * Create a new TwitterFileSpout.
	 * @param fileName The name of the file to read tweets from.
	 */
	public TwitterFileSpout(String fileName) {
		_fileName = fileName;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
		// Open the tweets file.
		try {
			_tweetFileReader = new BufferedReader(new InputStreamReader(
					TwitterFileSpout.class.getClassLoader().getResourceAsStream(_fileName)));
		} catch (Throwable e) {
			getLogger().error("Unable to open file " + _fileName 
					+ " in the 'input' directory to read tweets.");
			System.exit(1);
		}
			
	}

	@Override
	public void nextTuple() {
		String line = null;
		try {
			if ((line = _tweetFileReader.readLine()) != null) {
				_collector.emit(new Values(DataObjectFactory.createStatus(line)));
			} else {
				// Finished reading the file, exit the program.
				System.exit(0);
			}
		} catch (IOException e) {
			getLogger().error("Unable to read from input tweet input file: " + _fileName);
			System.exit(1);
		} catch (TwitterException e) {
			// If we can't process an individual tweet JSON.
			++_linesSkipped;
			getLogger().warn("Couldn't parse a JSON line from the input file. Make sure your JSON is well formed. Skipped " + _linesSkipped + " lines so far.");
			nextTuple();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status"));
	}

}
