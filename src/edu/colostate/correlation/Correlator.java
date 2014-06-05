package edu.colostate.correlation;

import edu.colostate.correlation.input.GalileoInputFormat;
import edu.colostate.correlation.map.CorrelationKey;
import edu.colostate.correlation.map.CorrelationMapper;
import edu.colostate.correlation.map.CorrelationReducer;
import edu.colostate.correlation.map.CorrelationValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 11:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class Correlator {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setStrings("input_file", args[0]);
        conf.setStrings("input_folder", args[1]);

        try {
            Job job = new Job(conf, "Correlation");
            job.setJarByClass(Correlator.class);
            job.setMapperClass(CorrelationMapper.class);
            job.setReducerClass(CorrelationReducer.class);
            job.setMapOutputKeyClass(CorrelationKey.class);
            job.setMapOutputValueClass(CorrelationValue.class);
            job.setOutputKeyClass(CorrelationKey.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setInputFormatClass(GalileoInputFormat.class);
            job.setNumReduceTasks(128);

            FileOutputFormat.setOutputPath(job, new Path("result"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
