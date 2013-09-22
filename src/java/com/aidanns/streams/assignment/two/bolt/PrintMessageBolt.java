package com.aidanns.streams.assignment.two.bolt;

import java.util.Map;

import twitter4j.Status;
import twitter4j.internal.logging.Logger;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * PrintMessageBolt accepts a message and prints it to the logger as a
 * message with INFO precedence.
 */
public class PrintMessageBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = -2810876355107441445L;
	
	private Logger _logger;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context) {
		_logger = Logger.getLogger(PrintMessageBolt.class);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Status status = (Status) tuple.getValue(0);
		_logger.info(status.getText());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output.
		return;
	}
}