package com.dataflow.storm;

import org.apache.http.HttpHost;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Code adapted from the https://github.com/DigitalPebble/storm-crawler project - TODO: talk with Julien about creating a generic implementation.
 */
public class MetricsConsumer implements IMetricsConsumer {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String indexName;

    private RestHighLevelClient elastic;
    private BulkProcessor bulkProcessor;

    private String stormID;

    private SimpleDateFormat dateFormat;

    @Override
    public void prepare(final Map conf,
                        final Object registrationArgument,
                        final TopologyContext ctx,
                        final IErrorReporter errorReporter) {
        indexName = "storm-metrics";
        stormID = ctx.getStormId();
        if (registrationArgument != null) {
            dateFormat = new SimpleDateFormat((String) registrationArgument);
            log.info("Using date format {}", registrationArgument);
        }

        try {
            final String host = "localhost";
            final String port = "9200";
            elastic = new RestHighLevelClient(RestClient.builder(new HttpHost(host, Integer.parseInt(port), "http")));
        }
        catch (Exception e) {
            log.error("Can't connect to ElasticSearch", e);
            throw new RuntimeException(e);
        }

        final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(final long executionId, final BulkRequest request) {
                final int numberOfActions = request.numberOfActions();
                log.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(final long executionId, final BulkRequest request, final BulkResponse response) {
                if (response.hasFailures()) {
                    log.warn("Bulk [{}] executed with failures", executionId);
                }
                else {
                    log.debug("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(final long executionId, final BulkRequest request, final Throwable failure) {
                log.error("Failed to execute bulk", failure);
            }
        };

        final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> elastic.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        final BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);

        builder.setBulkActions(500);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(0);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        bulkProcessor = builder.build();
    }

    @Override
    public void cleanup() {
        if (elastic != null) {
            try {
                bulkProcessor.close();
                elastic.close();
            }
            catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public void handleDataPoints(final TaskInfo taskInfo, final Collection<DataPoint> dataPoints) {
        final Date now = new Date();
        for (DataPoint dataPoint : dataPoints) {
            handleDataPoints(taskInfo, dataPoint.name, dataPoint.value, now);
        }
    }

    private void handleDataPoints(final TaskInfo taskInfo,
                                  final String name,
                                  final Object value,
                                  final Date now) {
        if (value instanceof Number) {
            indexDataPoint(taskInfo, now, name, ((Number) value).doubleValue());
        }
        else if (value instanceof Map) {
            final Iterator<Map.Entry> entries = ((Map) value).entrySet().iterator();
            while (entries.hasNext()) {
                final Map.Entry entry = entries.next();
                final String nameprefix = name + "." + entry.getKey();
                handleDataPoints(taskInfo, nameprefix, entry.getValue(), now);
            }
        }
        else if (value instanceof Collection) {
            for (Object collectionObj : (Collection) value) {
                handleDataPoints(taskInfo, name, collectionObj, now);
            }
        }
        else {
            log.warn("Found data point value {} of {}", name, value.getClass().toString());
        }
    }

    /**
     * Returns the name of the index that metrics will be written to.
     *
     * @return elastic index name
     */
    private String getIndexName(final Date timestamp) {
        final StringBuilder sb = new StringBuilder(indexName);
        if (dateFormat != null) {
            sb.append("-").append(dateFormat.format(timestamp));
        }
        return sb.toString();
    }

    private void indexDataPoint(final TaskInfo taskInfo,
                                final Date timestamp,
                                final String name,
                                final double value) {
        try {
            final XContentBuilder builder = jsonBuilder().startObject();
            builder.field("stormId", stormID);
            builder.field("srcComponentId", taskInfo.srcComponentId);
            builder.field("srcTaskId", taskInfo.srcTaskId);
            builder.field("srcWorkerHost", taskInfo.srcWorkerHost);
            builder.field("srcWorkerPort", taskInfo.srcWorkerPort);
            builder.field("name", name);
            builder.field("value", value);
            builder.field("timestamp", timestamp);
            builder.endObject();

            final IndexRequest indexRequest = new IndexRequest(getIndexName(timestamp), "datapoint").source(builder);
            bulkProcessor.add(indexRequest);
        }
        catch (Exception e) {
            log.error("problem when building request for ES", e);
        }
    }
}