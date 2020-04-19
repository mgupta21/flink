package org.java.flink.concepts;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.java.flink.datamodel.SensorReading;
import org.java.flink.functions.CustomMapFunction;

public class Transformations {

    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    private void demo() {
        DataStream<SensorReading> readings = inputSensorStream();

        // Map Transformations
        DataStream<String> map1 = readings.map(r -> r.id);
        DataStream<String> map2 = readings.map(new CustomMapFunction());

        // Filter Transformation
        DataStream<SensorReading> filter = readings.filter(r -> r.temperature >= 25);

        // similar to map, but it can produce zero, one, or more output events for each incoming event
        DataStream<String> sentences = inputTextStream();
        // DataStream<String> words = sentences.flatMap(s -> s.split(" "));

        // Keyed Stream
        KeyedStream<SensorReading, String> keyedStream = readings.keyBy(r -> r.id);

        // Rolling Aggregations
        // input stream is keyed by the first field and the rolling sum is computed on the second field.
        // Output: (1,2,2) -> (1,7,2) for key 1 & (2,3,1) -> (2,5,1) for key 2
        DataStream<Tuple3<Integer, Integer, Integer>> summation = intSteam().keyBy(0).sum(1);

        //  lambda reduce function forwards the first tuple field (the key field) and concatenates the List[String] values of the second tuple field.
        // langKeyedStream().keyBy(0).reduce((x, y) -> )

    }

    private DataStream<SensorReading> inputSensorStream() {
        return null;
    }

    private DataStream<String> inputTextStream() {
        return null;
    }

    private DataStream<Tuple3<Integer, Integer, Integer>> intSteam() {
        return env.fromElements(
            Tuple3.of(1, 2, 2), Tuple3.of(2, 3, 1), Tuple3.of(2, 2, 4), Tuple3.of(1, 5, 3));
    }

    private DataStream<Tuple2<String, List<String>>> langKeyedStream() {
        return env.fromElements(
            Tuple2.of("en", Collections.singletonList("tea")), Tuple2.of("fr", Collections.singletonList("vin")), Tuple2.of("en", Collections.singletonList("cake")));
    }

}
