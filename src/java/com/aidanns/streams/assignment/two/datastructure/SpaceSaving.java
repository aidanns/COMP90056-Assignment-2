package com.aidanns.streams.assignment.two.datastructure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * A simple implementation of the SpaceSaving algorithm presented in lectures.
 * @author Aidan Nagorcka-Smith (aidann@student.unimelb.edu.au)
 */
public class SpaceSaving<T> {
	
	public static class Counter<E> implements Comparable<Counter<E>> {
		
		private E _item;
		private Integer _count;
		private Integer _error;
		
		/**
		 * Create a new counter.
		 * @param item The item.
		 * @param count It's count.
		 * @param error Maximum error in the count.
		 */
		public Counter(E item, int count, int error) {
			_item = item;
			_count = count;
			_error = error;
		}
		
		public E getItem() {
			return _item;
		}
		
		public Integer getCount() {
			return _count;
		}
		
		public Integer getError() {
			return _error;
		}
		
		@Override
		public int hashCode() {
			return _item.hashCode();
		}
		
		@Override
		public boolean equals(Object other) {
			if (other instanceof Counter == false) {
				return false;
			}
			@SuppressWarnings("unchecked")
			Counter<E> otherCounter = (Counter<E>) other;
			return otherCounter.getItem().equals(_item);
		}

		@Override
		public int compareTo(Counter<E> o) {
			return _count.compareTo(o.getCount());
		}
		
		public String toString() {
			return "Counter <Item: " + _item.toString() + ", Count: " + _count + ", Error: " + _error + ">";
		}
	}

	/** Maximum number of items to store. Larger is more accurate. */
	private int _size;
	
	/** Map from the items to their count. */
	private Map<T, Integer> _itemToCount = new HashMap<T, Integer>();
	
	/** Map from the items to the maximum error in their count. */
	private Map<T, Integer> _itemToError = new HashMap<T, Integer>();
	
	/**
	 * Create a SpaceSaving data structure.
	 * @param size Maximum number of items to store accurate counts for.
	 */
	public SpaceSaving(int size) {
		_size = size;
	}
	
	private int removeSmallest() {
		Integer smallestCount = null;
		T smallestItem = null;
		for (T item : _itemToCount.keySet()) {
			if (smallestCount == null) {
				smallestCount = _itemToCount.get(item);
				smallestItem = item;
			} else {
				if (_itemToCount.get(item) < smallestCount) {
					smallestCount = _itemToCount.get(item);
					smallestItem = item;
				}
			}
		}
		_itemToCount.remove(smallestItem);
		_itemToCount.remove(smallestItem);
		return smallestCount;
	}
	
	public void offer(T item) {
		if (_itemToCount.containsKey(item)) {
			int newValue = _itemToCount.get(item) + 1;
			_itemToCount.put(item, newValue);

		} else {
			if (_itemToCount.size() < _size) {
				_itemToCount.put(item, 1);
				_itemToError.put(item, 0);
			} else {
				int previousSmallestCount = removeSmallest();
				_itemToCount.put(item, previousSmallestCount + 1);
				_itemToError.put(item, previousSmallestCount);
			}
		}
	}
	
	public List<Counter<T>> topK(int k) {
		PriorityQueue<Counter<T>> queue = new PriorityQueue<Counter<T>>(_itemToCount.size(), new Comparator<Counter<T>>() {
			@Override
			public int compare(Counter<T> o1, Counter<T> o2) {
				return o2.compareTo(o1); // Inverse the ordering because we want a max heap.
			}
		});
		for (T item : _itemToCount.keySet()) {
			Counter<T> counter = new Counter<T>(item, _itemToCount.get(item), _itemToError.get(item));
			queue.add(counter);
		}
		
		List<Counter<T>> topK = new ArrayList<Counter<T>>();
		int m = Math.min(k, queue.size()); // Might not have got k elements yet.
		for (int i = 0; i < m; i++) {
			topK.add(queue.poll());
		}
		return topK;
	}
	
}
