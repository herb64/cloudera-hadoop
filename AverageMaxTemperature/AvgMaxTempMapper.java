import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// From Data file format
//
// ** Data File Format **
// 
// The data file format is shown below.
// 
// Column,         Explanation
// --------------------------------------------------------------------
//  1              Product code
//  2              Bureau of Meteorology station number
//  3              Year
//  4              Month
//  5              Day
//  6              Daily maximum temperature (degrees Celsius)
//  7              Period over which daily maximum temperature was measured (days)
//  8              Quality of daily maximum temperature


public class AvgMaxTempMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

        // OWN Implementation based on sample solutions
        
        // read the line
        String line = value.toString();
        // split int words array of strings using separator ","
        String[] words = line.split(",");
        // Get the temperature value into a double variable "maxtemp"
        try {
                // the 6th column is of interest, so use index 5, as we start with 0
                double maxtemp = Double.parseDouble(words[5]);
                output.collect(new Text(words[3]), new DoubleWritable(maxtemp));
        }
        // catch exceptions by just ignoring the line and doing nothing
        catch (ArrayIndexOutOfBoundsException e) {}
        catch (NumberFormatException e) {}
    }
}
