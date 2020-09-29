package flink.benchmark;

import flink.benchmark.generator.HighKeyCardinalityGeneratorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import flink.benchmark.utils.Tuple3;
import flink.benchmark.utils.Tuple8;
import org.apache.flink.api.java.functions.KeySelector;
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

import java.io.IOException;

public class AdvertisingTopologyMock {
    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyMock.class);

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.fromInputStream(AdvertisingTopologyMock.class.getResourceAsStream("/benchmarkConf.yaml"));
        // flink environment
        StreamExecutionEnvironment env = setupFlinkEnvironment(config);
        DataStream<String> rawMessageStream = streamSource(config, env, 1);
        rawMessageStream.flatMap(new DeserializeBolt())
                .filter(new EventFilterBolt())
                .map(new MyMapFunction())
                .keyBy(new MyKeyBySelector())
                .flatMap(new CampaignProcessor(config));
        env.execute();
    }


    public static class MyKeyBySelector implements KeySelector<Object, String> {

        @Override
        public String getKey(Object input) throws Exception {
            return ((Tuple3<String, String, Object>)input).getF0();
        }
    }
    public static class MyMapFunction implements MapFunction<Tuple8<String, String, String, String, String, String, String, Object>, Object> {
        @Override
        public Tuple3<String, String, Object> map(Tuple8<String, String, String, String, String, String, String, Object> input) throws Exception {
            return Tuple3.of(input.getF2(), input.getF5(), input.getF7());
        }
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Object, String> {

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
        public void flatMap(Object obj, Collector<String> out) throws Exception {

            Tuple3<String, String, Object> tuple = (Tuple3<String, String, Object>)obj;
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
            FilterFunction<Tuple8<String, String, String, String, String, String, String, Object>> {
        @Override
        public boolean filter(Tuple8<String, String, String, String, String, String, String, Object> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    /**
     * Parse JSON
     */
    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple8<String, String, String, String, String, String, String, Object>> {

        @Override
        public void flatMap(String input, Collector<Tuple8<String, String, String, String, String, String, String, Object>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple8<String, String, String, String, String, String, String, Object> tuple =
                    new Tuple8<>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("campaign_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address"),
                            new Object());
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
