package com.roseline.hadoop.mapreduce.custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SensorKey implements WritableComparable {
    private Text type, equip_id, location;

    public SensorKey(){
        this.type = new Text();
        this.equip_id = new Text();
        this.location = new Text();
    }

    public SensorKey(Text type, Text equip_id, Text location) {
        this.type = type;
        this.equip_id = equip_id;
        this.location = location;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        type.write(out);
        equip_id.write(out);
        location.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type.readFields(in);
        equip_id.readFields(in);
        location.readFields(in);
    }

    @Override
    public int compareTo(Object o) {
        SensorKey other = (SensorKey) o;
        int comp = type.compareTo(other.type);
        if (comp != 0) {return comp;}

        comp = equip_id.compareTo(other.equip_id);
        if (comp != 0) { return comp;}

        return location.compareTo(other.location);
    }

    public Text getEquip_id() {
        return equip_id;
    }

    public Text getLocation() {
        return location;
    }

    public Text getType() {
        return type;
    }

    public void setEquip_id(Text equip_id) {
        this.equip_id = equip_id;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public void setType(Text type) {
        this.type = type;
    }
}
