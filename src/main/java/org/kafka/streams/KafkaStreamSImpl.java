package org.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * title        :
 * author       : sim
 * date         : 2023-11-17
 * description  :
 */
public class KafkaStreamSImpl {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // 스트림 포톨로지 정의를 위해
        KStream<String, String> streamLog = builder.stream(STREAM_LOG); // stream_log 토픽으로부터 KStream 객체 생성
        streamLog.to(STREAM_LOG_COPY); // KStream 객체를 stream_log_copy 토픽으로 전소'ㅇ

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
