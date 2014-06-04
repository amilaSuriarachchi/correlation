package edu.colostate.correlation.map;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 10:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class CorrelationKey implements WritableComparable<CorrelationKey> {

    //we assume all keys are sorted in alphabatical order
    private String field1;
    private String field2;

    public CorrelationKey() {
    }

    public CorrelationKey(String field1, String field2) {
        this.field1 = field1;
        this.field2 = field2;
    }

    @Override
    public int compareTo(CorrelationKey key) {
        if (this.field1.compareTo(key.field1) == 0){
            return this.field2.compareTo(key.field2);
        } else {
            return this.field1.compareTo(key.field1);
        }
    }

    @Override
    public String toString() {
        return this.field1 + "," + this.field2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.field1);
        dataOutput.writeUTF(this.field2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.field1 = dataInput.readUTF();
        this.field2 = dataInput.readUTF();
    }

    public String getField1() {
        return field1;
    }

    public void setField1(String field1) {
        this.field1 = field1;
    }

    public String getField2() {
        return field2;
    }

    public void setField2(String field2) {
        this.field2 = field2;
    }
}
