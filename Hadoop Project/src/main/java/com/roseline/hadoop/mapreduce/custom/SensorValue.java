package com.roseline.hadoop.mapreduce.custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SensorValue implements WritableComparable {
    private Text date, reading;

    public SensorValue() {
        this.date = new Text();
        this.reading = new Text();
    }

    public SensorValue(Text date, Text reading) {
        this.date = date;
        this.reading = reading;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        date.write(out);
        date.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date.readFields(in);
        reading.readFields(in);
    }

    @Override
    public int compareTo(Object o) {
        SensorValue other = (SensorValue) o;

        int comp = date.compareTo(other.date);
        if (comp != 0) { return comp; }

        return reading.compareTo(other.reading);
    }

    public Text getDate() {
        return date;
    }

    public Text getReading() {
        return reading;
    }

    public void setDate(Text date) {
        this.date = date;
    }

    public void setReading(Text reading) {
        this.reading = reading;
    }
}
