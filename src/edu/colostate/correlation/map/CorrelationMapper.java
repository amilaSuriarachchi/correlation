package edu.colostate.correlation.map;

import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.feature.Feature;
import galileo.util.GeoHash;
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
        List<String> featureNames = new ArrayList<String>();
        for (Iterator<Feature> iter = value.getAttributes ().iterator(); iter.hasNext();){
            Feature feature = iter.next();
            featureNames.add(feature.getName());
        }
        Coordinates coordinates = value.getSpatialProperties().getCoordinates();
        String location = GeoHash.encode(coordinates.getLatitude(), coordinates.getLongitude(), 5);

        Collections.sort(featureNames);
        for (int i = 0; i < featureNames.size(); i++){
            for (int j = i + 1; j < featureNames.size() ; j++){
                 CorrelationKey correlationKey = new CorrelationKey(featureNames.get(i), featureNames.get(j), location);
                 CorrelationValue correlationValue = new CorrelationValue(value.getTemporalProperties().getStart(),
                                                                          value.getAttribute(featureNames.get(i)).getDouble(),
                                                                          value.getAttribute(featureNames.get(j)).getDouble());
                context.write(correlationKey, correlationValue);
            }
        }
    }
}
