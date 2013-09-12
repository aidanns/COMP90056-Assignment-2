package com.aidanns.streams.assignment.two.spout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.internal.logging.Logger;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.v3.Twitter4jStatusClient;

@SuppressWarnings("serial")
public class TwitterStreamSpout extends BaseRichSpout {

	/** The file that contains the OAuth authentication data for Twitter. */
	private static String TWITTER_PROPERTIES_FILE_NAME = "twitter.properties";
	
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
	
	/** Queue for the statuses that are read from the twitter stream. */
	private BlockingQueue<Status> _statusQueue = 
			new LinkedBlockingQueue<Status>(100000);
	
	/** Output collector for this storm spout. */
	private SpoutOutputCollector _collector;

	/*
	 * (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, 
			TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		
		// Setup the hosebird client.
		BlockingQueue<String> messageQueue =
				new LinkedBlockingQueue<String>(100000);
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
		
		// Only send English tweets.
		List<String> languages = new ArrayList<String>();
		languages.add("en");
		endpoint.addQueryParameter(
				Constants.LANGUAGE_PARAM, Joiner.on(",").join(languages));
		
		// Load the OAuth authentication stuff from disk.
		Properties twitterProperties = new Properties();
		try {
			twitterProperties.load(TwitterStreamSpout.class.getClassLoader()
					.getResourceAsStream(TWITTER_PROPERTIES_FILE_NAME));
		} catch (IOException e) {
			getLogger().error("Failed to open file with twitter authentication"
					+ " credentials: " + TWITTER_PROPERTIES_FILE_NAME + "."
					+ " Ensure that it is on your classpath.");
			System.exit(1);
		}
		
		String consumerKey = twitterProperties.getProperty("oauth.consumerKey");
		String consumerSecret =	
				twitterProperties.getProperty("oauth.consumerSecret");
		String accessToken = 
				twitterProperties.getProperty("oauth.accessToken");
		String accessTokenSecret = 
				twitterProperties.getProperty("oauth.accessTokenSecret");
		
		if (consumerKey == null || consumerSecret == null 
				|| accessToken == null || accessTokenSecret == null) {
			getLogger().error("Failed to read a require property from"
					+ " file: " + TWITTER_PROPERTIES_FILE_NAME + "\n"
					+ twitterProperties);
			System.exit(1);
		}
		
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,
				accessToken, accessTokenSecret);
		
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(messageQueue));
		Client hosebirdClient = builder.build();
		
		// Setup the Twitter4J client.
		ExecutorService executorServer = Executors.newCachedThreadPool();
		List<StatusListener> statusListeners = new ArrayList<StatusListener>();
		statusListeners.add(new StatusListener() {

			@Override
			public void onException(Exception e) {
				Logger.getLogger(TwitterStreamSpout.class)
						.error(e.getMessage());
				return;
			}

			@Override
			public void onScrubGeo(long lat, long lon) {
				return;
			}

			@Override
			public void onStatus(Status status) {
				Logger.getLogger(TwitterStreamSpout.class)
						.debug(status.getText());
				_statusQueue.add(status);
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				return;
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				return;
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				return;
			}
			
		});
		Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
				hosebirdClient,	messageQueue, statusListeners, executorServer);
		
		// Start the client.
		t4jClient.connect();
		t4jClient.process();
	}

	/*
	 * (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		Status status = _statusQueue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(status));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status"));
	}

}
