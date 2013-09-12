package com.aidanns.streams.assignment.two.bolt;

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public abstract class BaseStatusBolt extends BaseRichBolt {

	/** Collector to use for outptut. */
	private OutputCollector _collector;
	
	/*
	 * (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status"));
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		processStatus((Status) input.getValue(0));
	}
	
	/**
	 * Emit a status from the bolt.
	 * @param status The status to emit.
	 */
	protected void emit(Status status) {
		_collector.emit(new Values(status));
	}
	
	/**
	 * Process a status and output it.
	 * @param status The input status.
	 * @param collector Collectors to use for output.
	 */
	abstract void processStatus(Status status);

}
