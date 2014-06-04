package edu.colostate.correlation.input;

import galileo.dataset.Metadata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/3/14
 * Time: 3:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class GalileoRecordReader extends RecordReader<IntWritable, Metadata> {

    private GalileoReader galileoReader;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.galileoReader = new GalileoDirectoryReader(new File("/tmp/amila/galileo"));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return this.galileoReader.hasNext();
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        // we don't interested any pirticular key
        return new IntWritable(0);
    }

    @Override
    public Metadata getCurrentValue() throws IOException, InterruptedException {
        return this.galileoReader.nextValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
