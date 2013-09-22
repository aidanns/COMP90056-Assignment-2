package com.aidanns.streams.assignment.two.topology;

import java.util.Properties;

import twitter4j.internal.logging.Logger;
import com.aidanns.streams.assignment.two.spout.TwitterFileSpout;
import com.aidanns.streams.assignment.two.spout.TwitterStreamSpout;

/**
 * Main class to run the storm job, reading from a local file.
 * Runs in a local cluster.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
public class AssignmentTwoFromFile {
	
	/** The file that contains the filename to read tweets from. */
	private static String INPUT_FILE_PROPERTIES_FILE_NAME = "input.properties";
	
	public static void main(String[] args) {
		// Load the name of the input file from disk.
		Properties inputProperties = new Properties();
		try {
			inputProperties.load(AssignmentTwoFromFile.class.getClassLoader()
					.getResourceAsStream(INPUT_FILE_PROPERTIES_FILE_NAME));
		} catch (Throwable e) {
			Logger.getLogger(TwitterStreamSpout.class).error("Failed to open file specifying the name of the"
					+ " input file: " + INPUT_FILE_PROPERTIES_FILE_NAME);
			System.exit(1);
		}
		String inputFileName = inputProperties.getProperty("input.tweets.filename");

		AssignmentTwo.runApplicationWithSpout(new TwitterFileSpout(inputFileName));
	}
	
}
