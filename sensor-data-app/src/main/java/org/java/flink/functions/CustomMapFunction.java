package org.java.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.java.flink.datamodel.SensorReading;

public class CustomMapFunction implements MapFunction<SensorReading, String> {

    @Override
    public String map(SensorReading s) {
        return s.id;
    }

    public static void main(String[] args) {

    }
}
