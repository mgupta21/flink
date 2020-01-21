package org.java.flink.util;

import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.java.flink.datamodel.SensorReading;

import static java.util.stream.Collectors.toList;

public class SensorReadingsManager {

    private Random   rand = new Random();
    private String[] sensorIds;
    private double[] curFTemp;

    public SensorReadingsManager(int numberOfSensors) {
        this.sensorIds = new String[numberOfSensors];
        this.curFTemp = new double[numberOfSensors];
    }

    public void initializeSensorReadings(int taskIdx) {
        // initialize sensor ids and temperatures
        IntStream.range(0, 10).forEach(i -> {
            sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        });
    }

    public void updateSensorReadings() {
        IntStream.range(0, sensorIds.length).forEach(i -> {
            // update current temperature
            curFTemp[i] += rand.nextGaussian() * 0.5;
        });
    }

    public List<SensorReading> getSensorReadings() {
        // get current time
        long curTime = Calendar.getInstance().getTimeInMillis();
        return IntStream.range(0, sensorIds.length).mapToObj(i -> new SensorReading(sensorIds[i], curTime, curFTemp[i])).collect(toList());
    }

}
