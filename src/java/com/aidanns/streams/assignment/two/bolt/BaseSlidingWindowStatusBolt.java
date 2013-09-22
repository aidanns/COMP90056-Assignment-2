package com.aidanns.streams.assignment.two.bolt;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.google.api.client.util.Lists;

import twitter4j.Status;

/**
 * Base implementation of windowing for a bolt that processes Twitter statuses.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
@SuppressWarnings("serial")
public abstract class BaseSlidingWindowStatusBolt extends BaseStatusBolt {
	
	/** The current state of the window. */
	private LinkedList<Status> _window = new LinkedList<Status>();
	
	/** The size of the window that will be passed to the processWindow method */
	private int _windowSizeInSeconds;
	
	/**
	 * Create a new BaseSlidingWindowBolt.
	 * @param windowSizeInSeconds The number of seconds of data to pass in to
	 *     the processWindow method.
	 */
	public BaseSlidingWindowStatusBolt(int windowSizeInSeconds) {
		_windowSizeInSeconds = windowSizeInSeconds;
	}
	
	/**
	 * Check whether the given date is out of the window.
	 * @param date The date to check.
	 * @param currentDate The current date.
	 * @return If the date is out of the current window.
	 */
	private boolean dateIsOutOfWindow(Date date, Date currentDate) {
		return (currentDate.getTime() - date.getTime()) > _windowSizeInSeconds * 1000;
	}
	
	/**
	 * Override this method to implement the logic that you want to run for 
	 * every window that needs to be processed.
	 * @param window The window of statuses that will be processed. The window
	 *     can not be modified.
	 * @param additions The statuses that were added since the last call to processWindow.
	 * @param deletions The statuses that were removed since the last call to processWindow.
	 */
	protected abstract void processWindow(List<Status> window, List<Status> additions,
			List<Status> deletions);

	@Override
	void processStatus(Status status) {
		_window.push(status);
		
		List<Status> additions = Lists.newArrayList();
		additions.add(status);
		
		List<Status> deletions = Lists.newArrayList();
		
		// Remove any statuses that are now out of date.
		while (_window.peek() != null && dateIsOutOfWindow(_window.peek().getCreatedAt(), status.getCreatedAt())) {
			deletions.add(_window.pop());
		}
		
		// Process all statuses in the current window.
		processWindow(Collections.unmodifiableList(_window),
				Collections.unmodifiableList(additions),
				Collections.unmodifiableList(deletions));
	}

}
