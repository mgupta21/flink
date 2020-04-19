package org.java.flink.app;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.java.flink.datamodel.SensorReading;
import org.java.flink.source.SensorSource;

public class AverageSensorReadings {

    /**
     * main() defines and executes the DataStream program.
     *
     * @param args
     *            program arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // The execution environment determines whether the program is running on a local machine or on a cluster
        // StreamExecutionEnvironment.createLocalEnvironment();
        // config: Host name of Job manager, port of job manager process, jar file to ship to Jab Manager
        // StreamExecutionEnvironment.createRemoteEnvironment("host", 1234, "path/to/jarFile.jar");

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        // Assigning watermarks at source
        DataStream<SensorReading> sensorData = env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // Some transformations can produce a new DataStream, possibly of a different type, while other transformations do not modify the records of
        // the DataStream but reorganize it by partitioning or grouping.
        DataStream<SensorReading> avgTemp = sensorData
            // transformation to convert Fahrenheit to Celsius using and inlined map function
            .map(r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
            // transformation to partition the sensor readings by their id. organize stream by sensor id
            .keyBy(r -> r.id)
            // transformation to group the sensor readings of each sensor ID partition into tumbling windows of 1 second
            .timeWindow(Time.seconds(1))
            // compute average temperature using a user-defined window function
            .apply(new TemperatureAverager());

        // print result stream to standard out
        avgTemp.print();

        // Flink programs are executed lazily. Only when execute() is called does the system trigger the execution of the program.
        env.execute("Compute average sensor temperature");
    }

    /**
     * User-defined WindowFunction to compute the average temperature of SensorReadings
     */
    public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        /**
         * apply() is invoked once for each window.
         *
         * @param sensorId
         *            the key (sensorId) of the window
         * @param window
         *            meta data for the window
         * @param input
         *            an iterable over the collected sensor readings that were assigned to the window
         * @param out
         *            a collector to emit results from the function
         */
        @Override
        public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) {

            // compute the average temperature
            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : input) {
                cnt++;
                sum += r.temperature;
            }
            double avgTemp = sum / cnt;

            // emit a SensorReading with the average temperature and timestamp of window end
            out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
        }
    }
}