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

    private String[] locations;
    private String folder;

    public GalileoInputSplit() {

    }

    public GalileoInputSplit(String[] locations, String folder) {
        this.locations = locations;
        this.folder = folder;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return this.locations;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.locations.length);
        for (int i = 0; i < this.locations.length; i++ ) {
            dataOutput.writeUTF(this.locations[i]);
        }
        dataOutput.writeUTF(this.folder);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        this.locations = new String[size];
        for (int i = 0 ; i < this.locations.length ; i++) {
            this.locations[i] = dataInput.readUTF();
        }
        this.folder = dataInput.readUTF();
    }
}
