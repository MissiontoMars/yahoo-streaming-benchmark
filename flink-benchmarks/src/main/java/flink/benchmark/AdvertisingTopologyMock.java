package flink.benchmark;

import flink.benchmark.generator.HighKeyCardinalityGeneratorSource;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AdvertisingTopologyMock {
    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyMock.class);

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.fromInputStream(AdvertisingTopologyMock.class.getResourceAsStream("/benchmarkConf.yaml"));
        // flink environment
        StreamExecutionEnvironment env = setupFlinkEnvironment(config);
        DataStream<String> rawMessageStream = streamSource(config, env, 1);
        rawMessageStream.flatMap(new DeserializeBolt())
                .filter(new EventFilterBolt())
                .<Tuple2<String, String>>project(2, 5)
                .keyBy(0)
                .flatMap(new CampaignProcessor(config));
        env.execute();
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Tuple2<String, String>, String> {

        BenchmarkConfig config;
        private int eventCount = 0;

        CampaignProcessor(BenchmarkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) {
            LOG.info("Opening connection with Jedis to {}", config.redisHost);
        }

        @Override
        public void flatMap(Tuple2<String, String> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(0);
            String event_time = tuple.getField(1);

            eventCount++;
            if (eventCount % 10000 == 0)
                LOG.info("CampaignProcessor2 flatMap campaign_id: {} event_time: {} eventCount: {}", campaign_id, event_time, eventCount);
        }
    }

    /**
     * Filter down to only "view" events
     */
    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    /**
     * Parse JSON
     */
    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("campaign_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address"));
            out.collect(tuple);
        }
    }

    /**
     * Choose data source, either Kafka or data generator
     */
    private static DataStream<String> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env, int parallelism) {
        RichParallelSourceFunction<String> source;
        String sourceName;
        if (config.useLocalEventGenerator) {
            HighKeyCardinalityGeneratorSource eventGenerator = new HighKeyCardinalityGeneratorSource(config);
            source = eventGenerator;
            sourceName = "EventGenerator";
        } else {
            source = null;
            sourceName = "Kafka";
        }

        return env.addSource(source, sourceName);
    }

    /**
     * Do some Flink Configuration
     */
    private static StreamExecutionEnvironment setupFlinkEnvironment(BenchmarkConfig config) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(config.getParameters());
        env.getConfig().enableObjectReuse();

        // enable checkpointing for fault tolerance
        if (config.checkpointsEnabled) {
            env.enableCheckpointing(config.checkpointInterval);
            if (config.checkpointToUri) {
                env.setStateBackend(new FsStateBackend(config.checkpointUri));
            }
        }

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

}
