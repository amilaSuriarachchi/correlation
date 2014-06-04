package edu.colostate.correlation.map;

import galileo.dataset.Metadata;
import galileo.dataset.feature.Feature;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 9:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class CorrelationMapper extends Mapper<IntWritable, Metadata,CorrelationKey, CorrelationValue> {

    @Override
    protected void map(IntWritable key, Metadata value, Context context) throws IOException, InterruptedException {

        //all all the features to a list to sort them out
        System.out.println("================== Within the mapper task ==================");
        List<String> featureNames = new ArrayList<String>();
        for (Iterator<Feature> iter = value.getAttributes ().iterator(); iter.hasNext();){
            Feature feature = iter.next();
            featureNames.add(feature.getName());
        }

        Collections.sort(featureNames);
        for (int i = 0; i < featureNames.size(); i++){
            for (int j = i + 1; j < featureNames.size() ; j++){
                 CorrelationKey correlationKey = new CorrelationKey(featureNames.get(i), featureNames.get(j));
                 CorrelationValue correlationValue = new CorrelationValue(value.getTemporalProperties().getStart(),
                                                                          value.getAttribute(featureNames.get(i)).getDouble(),
                                                                          value.getAttribute(featureNames.get(j)).getDouble());
                System.out.println("======================= writting to the context ========================");
                context.write(correlationKey, correlationValue);
            }
        }
    }
}
