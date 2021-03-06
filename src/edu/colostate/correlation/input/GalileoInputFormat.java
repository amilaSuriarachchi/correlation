package edu.colostate.correlation.input;

import galileo.dataset.Metadata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/3/14
 * Time: 2:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class GalileoInputFormat extends InputFormat<IntWritable, Metadata> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        List<InputSplit> inputSplits = new ArrayList<InputSplit>();
        Scanner scanner = new Scanner(new File(jobContext.getConfiguration().getStrings("input_file")[0]));

        List<String> locations = new ArrayList<String>();
        while (scanner.hasNext()) {
            locations.add(scanner.next());
        }
        scanner.close();

        String[] locationsArray = locations.toArray(new String[locations.size()]);

        String inputFolder = jobContext.getConfiguration().getStrings("input_folder")[0];
        //we going to assume data will be there only for one year and create splits per month
        for (int i = 1; i < 13; i++) {
            inputSplits.add(new GalileoInputSplit(locationsArray, inputFolder + "/" + i));
        }
        return inputSplits;
    }

    @Override
    public RecordReader<IntWritable, Metadata> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new GalileoRecordReader();
    }
}
