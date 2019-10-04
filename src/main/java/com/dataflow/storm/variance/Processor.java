package com.dataflow.storm.variance;

import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

import static org.apache.storm.utils.Utils.sleep;

public class Processor extends BaseBasicBolt {

    private static final String StragglerMode = "straggler";

    private MultiCountMetric eventCounter;

    private final Random random = new Random();

    private final String mode;
    private final int waitRange;
    private int toProcess;
    private int waitMillis;

    private boolean isStraggler = false;

    public Processor(final String mode, final int waitRange, final int waitMillis) {
        this.mode = mode;
        this.waitRange = waitRange;
        this.waitMillis = waitMillis;
        resetToProcess();
    }

    private void resetToProcess() {
        toProcess = random.nextInt(waitRange);
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context) {
        super.prepare(stormConf, context);

        if (mode.equals(StragglerMode) && context.getThisTaskId() == 6) {
            isStraggler = true;
        }

        this.eventCounter = context.registerMetric("varianceTest", new MultiCountMetric(), 5);
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        final int number = tuple.getInteger(0);
        eventCounter.scope("numCount").incrBy(1);

        if (!mode.equals(StragglerMode)) {
            sleep(number);
        }
        else if (isStraggler && --toProcess <= 0) {
            resetToProcess();
            sleep(waitMillis);
        }
        else {
            sleep(number);
        }

        collector.emit(new Values(number));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }
}