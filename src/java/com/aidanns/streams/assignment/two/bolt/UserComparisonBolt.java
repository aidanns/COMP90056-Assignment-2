package com.aidanns.streams.assignment.two.bolt;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import twitter4j.Status;
import twitter4j.User;

/**
 * A bolt that compares users pair-wise for similarity based on the algorithm
 * described in the assignment specification. Does so over a sliding window of
 * the last n seconds.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
@SuppressWarnings("serial")
public class UserComparisonBolt extends BaseSlidingWindowStatusBolt {

	/**
	 * Representation of the required record format for the output.
	 */
	private class SimilarityRecord {
		
		/** Time the similarity was first seen. */
		private Date _time;
		
		/** Name of the first user. */
		private String _userOneName;
		
		/** Name of the second user. */
		private String _userTwoName;
		
		/** Similarity score between the two users. */
		private double _similarityScore;
		
		/**
		 * Create a new similartiy record.
		 */
		public SimilarityRecord(Date time, String userOneName,
				String userTwoName, double similarityScore) {
			_time = time;
			_userOneName = userOneName;
			_userTwoName = userTwoName;
			_similarityScore = similarityScore;
		}
		
		public Date time() {
			return _time;
		}

		public String userOneName() {
			return _userOneName;
		}

		public String userTwoName() {
			return _userTwoName;
		}

		public double similarityScore() {
			return _similarityScore;
		}
		
		/**
		 * Two SimilarityScores are considered equal if they have the same score
		 * and are between the same users (doesn't need to be made at the same time).
		 * @param other The object to compare this one to.
		 * @return Whether the two objects are equal.
		 */
		@Override
		public boolean equals(Object other) {
			if (other instanceof SimilarityRecord == false) {
				return false;
			}
			
			SimilarityRecord otherRecord = (SimilarityRecord) other;
			
			return ((otherRecord.userOneName().equals(_userOneName) &&
					otherRecord.userTwoName().equals(_userTwoName) || 
					(otherRecord.userOneName().equals(_userTwoName) && 
					otherRecord.userTwoName().equals(_userOneName))) &&
					otherRecord.similarityScore() == _similarityScore);
		}

		/**
		 * Generate the hash code for this record from the two users that it's 
		 * comparing. Avoid using the time as a source of the hash as we don't
		 * use it for comparison.
		 * @return A hash for this object.
		 */
		@Override
		public int hashCode() {
			return _userOneName.hashCode() + _userTwoName.hashCode();
		}
	}
	
	/**
	 * A task that can be executed to write the output for this bolt to a file.
	 */
	private class OutputSimilarUsersTask extends TimerTask {
		
		private DateFormat _dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");

		@Override
		public void run() {
			Writer writer = null;
			
			try {
				writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(OUTPUT_FILE_NAME)));
				
				writer.write("Users Similarity Statistics:\n");
				writer.write("\n");
				// See the assignment spec for the output format.
				for (SimilarityRecord record : _records) {
					writer.write(_dateFormatter.format(record.time())+ ", " 
							+ record.userOneName() + ", " + record.userTwoName() 
							+ ", " + String.format("%.2f", record.similarityScore()) + "\n");
				}
			} catch (IOException e) {
				Logger.getLogger(OutputSimilarUsersTask.class).error(
						"Error while writing similarity statistics.");
			} finally {
				try { writer.close(); } catch (Throwable e) { /* Make sure it closed */ }
			}
		}
	}
	
	/** Delay before first write in ms. */
	private long FILE_WRITE_DELAY = 0;

	/** Delay between writes in ms. */
	private long FILE_WRITE_PERIOD = 1000;

	/** File to write output to. */
	private String OUTPUT_FILE_NAME = "output/similarity.txt";
	
	/** Set of previously recorded events of similar users. */
	private Set<SimilarityRecord> _records = new HashSet<SimilarityRecord>();
	
	/** The hurdle for being considered "similar" according to the assignment
	 * specification. Any users that have a score higher than this will have 
	 * that similarity recorded.
	 */
	private double _similarityHurdle;
	
	/**
	 * Create a new UserComparisonBolt.
	 * @param windowSizeInSeconds The number of seconds to consider in the past
	 *     when creating the window of statuses.
	 * @param similarityHurdle The hurdle for being considered "similar" according to the assignment
	 * specification. Any users that have a score higher than this will have 
	 * that similarity recorded.
	 */
	public UserComparisonBolt(int windowSizeInSeconds, double similarityHurdle) {
		super(windowSizeInSeconds);
		_similarityHurdle = similarityHurdle;
	}
	
	/**
	 * Compute the similarity score between two multisets of words, represented
	 * as maps from word to count.
	 * @param leftWords Words from the "left" group.
	 * @param rightWords Words from the "right" group.
	 * @return Similarity score between the two groups.
	 */
	private double computeSimilarityScore(Map<String, Integer> leftWords, Map<String, Integer> rightWords) {
		// See the assignment specification for this algorithm.
		double numerator = 0;
		double denomniatorLeft = 0;
		double denominatorRight = 0;
		
		for (String leftWord : leftWords.keySet()) {
			for (String rightWord : rightWords.keySet()) {
				if (leftWord.equals(rightWord)) {
					numerator += leftWords.get(leftWord) * rightWords.get(rightWord);
				}
			}
		}
		
		for (String leftWord : leftWords.keySet()) {
			denomniatorLeft += leftWords.get(leftWord) * leftWords.get(leftWord);
		}
		
		for (String rightWord : rightWords.keySet()) {
			denominatorRight += rightWords.get(rightWord) * rightWords.get(rightWord);
		}
		
		return numerator / (Math.sqrt(denomniatorLeft) * Math.sqrt(denominatorRight));
	}

	@Override
	protected void processWindow(List<Status> window) {
		
		// Map<"twitter username", Map<"word in their tweets", "frequency of that word in their tweets">>
		Map<User, Map<String, Integer>> userToWordsMap = new HashMap<User, Map<String, Integer>>();
		
		// Iterate through all the statuses and create a multiset that represents each user.
		for (Status s : window) {
			if (!userToWordsMap.containsKey(s.getUser())) {
				userToWordsMap.put(s.getUser(), new HashMap<String, Integer>());
			}
			Map<String, Integer> usersWords = userToWordsMap.get(s.getUser());
			StringTokenizer tokenizer = new StringTokenizer(s.getText());
			while (tokenizer.hasMoreElements()) {
				String word = tokenizer.nextToken();
				if (usersWords.containsKey(word)) {
					usersWords.put(word, usersWords.get(word) + 1);
				} else {
					usersWords.put(word, 1);
				}
			}
		}

		// Calculate similarity for all user pairs and record it if we havn't recorded the same score before.
		// We will generate the same similarity multiple times (because it's a sliding window), so we need to avoid storing multiple copies of that similarity record.
		for (User u1 : userToWordsMap.keySet()) {
			for (User u2 : userToWordsMap.keySet()) {
				if (!u1.equals(u2)) {
					double similarity = computeSimilarityScore(userToWordsMap.get(u1), userToWordsMap.get(u2));
					if (similarity > _similarityHurdle) {
						SimilarityRecord record = new SimilarityRecord(new Date(), u1.getName(), u2.getName(), similarity);
						if (!_records.contains(record)) {
							_records.add(record);
						}
					}
				}
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		Timer outputToFileTimer = new Timer();
		outputToFileTimer.scheduleAtFixedRate(
				new OutputSimilarUsersTask(), FILE_WRITE_DELAY, FILE_WRITE_PERIOD);
	}
	
	

}
