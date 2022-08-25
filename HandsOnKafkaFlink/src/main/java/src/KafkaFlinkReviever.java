package src;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class KafkaFlinkReviever {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("testtopic")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest()).build();

        final DataStream<String> kafkaData = env
                .fromSource(kafkaSource, WatermarkStrategy
                                .noWatermarks(),
                        "kafkaSource").setParallelism(1);



        kafkaData.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -999736771747691234L;

            @Override
            public String map(String value) throws Exception {
                return "Receiving from Kafka : " + value;
            }
        }).print();

        env.execute();
    }



}
