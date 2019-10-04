package com.dataflow.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RandomNumberSpout extends BaseRichSpout {

    private static final Logger log = LoggerFactory.getLogger(RandomNumberSpout.class);

    private final Random random = new Random();

    private final int intervalMillis;
    private final int lowerLimit;
    private final int range;

    private SpoutOutputCollector collector;

    public RandomNumberSpout(final int intervalMillis,
                             final int lowerLimit,
                             final int range) {
        this.intervalMillis = intervalMillis;
        this.lowerLimit = lowerLimit;
        this.range = range;
    }

    @Override
    public void open(final Map<String, Object> conf,
                     final TopologyContext context,
                     final SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(intervalMillis);
        collector.emit(new Values(lowerLimit + random.nextInt(range)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }
}