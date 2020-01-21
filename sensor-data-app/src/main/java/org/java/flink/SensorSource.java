package org.java.flink;

import java.util.List;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.java.flink.datamodel.SensorReading;
import org.java.flink.util.SensorReadingsManager;

/**
 * Flink SourceFunction to generate SensorReadings with random temperature values.
 *
 * Each instance of the source function simulates 10 sensors which emit one sensor reading every 100 ms.
 *
 * Note: This is a simple data-generating source function that does not checkpoint its state.
 * In case of a failure, the source does not replay any data.
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    // flag indicating whether source is still running
    private boolean               running               = true;
    private SensorReadingsManager sensorReadingsManager = new SensorReadingsManager(10);

    // run() continuously emits SensorReadings by emitting them through the SourceContext.
    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {

        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        sensorReadingsManager.initializeSensorReadings(taskIdx);

        while (running) {

            sensorReadingsManager.updateSensorReadings();
            List<SensorReading> sensorReadings = sensorReadingsManager.getSensorReadings();
            // emit SensorReadings
            sensorReadings.forEach(sourceContext::collect);

            // wait for 100 ms
            Thread.sleep(100);
        }
    }

    // Cancels this SourceFunction
    @Override
    public void cancel() {
        this.running = false;
    }

}
