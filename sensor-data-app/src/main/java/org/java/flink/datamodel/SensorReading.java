package org.java.flink.datamodel;

import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class SensorReading {
	public String id;
	public long timestamp;
	public double temperature;
}
