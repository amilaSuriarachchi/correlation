package edu.colostate.correlation.map;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 10:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class CorrelationValue implements Writable {

    private long timeStamp;
    private double field1;
    private double field2;

    public CorrelationValue() {
    }

    public CorrelationValue(long timeStamp, double field1, double field2) {
        this.timeStamp = timeStamp;
        this.field1 = field1;
        this.field2 = field2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timeStamp);
        dataOutput.writeDouble(this.field1);
        dataOutput.writeDouble(this.field2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timeStamp = dataInput.readLong();
        this.field1 = dataInput.readDouble();
        this.field2 = dataInput.readDouble();
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public double getField1() {
        return field1;
    }

    public void setField1(double field1) {
        this.field1 = field1;
    }

    public double getField2() {
        return field2;
    }

    public void setField2(double field2) {
        this.field2 = field2;
    }
}
