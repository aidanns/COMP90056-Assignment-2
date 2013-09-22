package com.aidanns.streams.assignment.two.topology;

import com.aidanns.streams.assignment.two.spout.TwitterStreamSpout;

/**
 * Main class to run the storm job reading from the Twitter streaming API.
 * Runs in a local cluster.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
public class AssignmentTwoFromStream {
	
	public static void main(String[] args) {
		AssignmentTwo.runApplicationWithSpout(new TwitterStreamSpout());
	}

}
