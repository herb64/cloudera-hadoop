import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class AvgMaxTempReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
   
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        // Implementation Herbert Mehlhose, based on sample solution code
        // Counter for the values, that we have processed. The "values" iterator
        // is passed to our reducer
        double count = 0;
        // here we sum up results
        double sum = 0;
        // We visit all values, unless there's no value left
        while (values.hasNext()) {
                // and add values to the sum
                sum += values.next().get();
                // counter is used to calculate the averate later on
                count++;
        }
        // we check, if there is a count at all, to avoid division by zero
        if (count > 0d) {
                // if any, just calculate the average by dividing sum by count
                double result = sum / count;
                // and put it into output
                output.collect(key, new DoubleWritable(result));
        }

    }
}
