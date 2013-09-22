package com.aidanns.streams.assignment.two.bolt;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import twitter4j.Status;

/**
 * Base implementation of windowing for a bolt that processes Twitter statuses.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
@SuppressWarnings("serial")
public abstract class BaseSlidingWindowStatusBolt extends BaseStatusBolt {
	
	/** Map from statuses to the date that they were fed to this bolt. */
	private Map<Status, Date> _statusToInsertionTimeMap = new HashMap<Status, Date>();
	
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
	 * @return If the date is out of the current window.
	 */
	private boolean dateIsOutOfWindow(Date date) {
		return (new Date().getTime() - date.getTime()) > _windowSizeInSeconds * 1000;
	}
	
	/**
	 * Override this method to implement the logic that you want to run for 
	 * every window that needs to be processed.
	 * @param window The window of statuses that will be processed. The window
	 *     can not be modified.
	 */
	protected abstract void processWindow(List<Status> window);

	@Override
	void processStatus(Status status) {
		_window.push(status);
		_statusToInsertionTimeMap.put(status, new Date());
		
		// Remove any statuses that are now out of date.
		while (_window.peek() != null && dateIsOutOfWindow(_statusToInsertionTimeMap.get(_window.peek()))) {
			_statusToInsertionTimeMap.remove(_window.peek());
			_window.pop();
		}
		
		// Process all statuses in the current window.
		processWindow(Collections.unmodifiableList(_window));
	}

}
