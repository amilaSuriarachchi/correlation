package edu.colostate.correlation.map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 6/4/14
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class CorrelationReducer extends Reducer<CorrelationKey, CorrelationValue, CorrelationKey, DoubleWritable> {

    @Override
    protected void reduce(CorrelationKey key, Iterable<CorrelationValue> values, Context context)
                                                                   throws IOException, InterruptedException {
        List<ValuePair> valuePairs = new ArrayList<ValuePair>();
        for (CorrelationValue value : values){
            valuePairs.add(new ValuePair(value.getField1(), value.getField2()));
        }

        context.write(key, new DoubleWritable(getCoefficient(valuePairs.size(), valuePairs.iterator())));

    }

    public double getCoefficient(int n, Iterator<ValuePair> iter) {
        double sumX = 0;
        double sumY = 0;
        double sumXX = 0;
        double sumYY = 0;
        double sumXY = 0;
        ValuePair valuePair = null;

        while (iter.hasNext()) {
            valuePair = iter.next();
            sumX += valuePair.getX();
            sumY += valuePair.getY();
            sumXX += valuePair.getX() * valuePair.getX();
            sumYY += valuePair.getY() * valuePair.getY();
            sumXY += valuePair.getX() * valuePair.getY();
        }

        return (n * sumXY - sumX * sumY) / (Math.sqrt(n * sumXX - sumX * sumX) * Math.sqrt(n * sumYY - sumY * sumY));
    }
}
