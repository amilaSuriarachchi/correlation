package edu.colostate.correlation.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/3/14
 * Time: 3:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class GalileoInputSplit extends InputSplit implements Writable {

    private String location;

    public GalileoInputSplit() {
    }

    public GalileoInputSplit(String location) {
        this.location = location;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{this.location};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.location);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.location = dataInput.readUTF();
    }
}
