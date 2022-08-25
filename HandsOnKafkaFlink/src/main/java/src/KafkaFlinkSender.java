package src;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class KafkaFlinkSender {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("testtopic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

//        DataStream<String> stringOutputStream = env.addSource(new StreamGenerator());
        DataStream<String>stringOutputStream=env.readTextFile("src/main/java/src/data/books.jsonl");
//        stringOutputStream.print();
        stringOutputStream.sinkTo(sink);

        env.execute("data Produced from a source");
    }

    public static class StreamGenerator implements SourceFunction<String> {

        boolean flag = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int counter = 0;
            while(flag) {
                ctx.collect("From Flink : "+ counter++);
                System.out.println("From Flink : "+ counter);
                Thread.sleep(1000);
            }
            ctx.close();
        }

        @Override
        public void cancel() {
            flag = false;
        }

    }


}