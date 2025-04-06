/**
 * Class Name: SiteLinkReducer
 * Author: Darcy Harper
 * Version 1.0
 * Course: ITEC 3170
 * Written: April 5, 2025
 * The purpose of this class is to implement the Reducer function
 * for a Hadoop MapReduce job that processes web link relationships.
 * It receives key-value pairs from the Mapper, removes duplicate links,
 * and outputs each website with the count of distinct links.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class SiteLinkReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        HashSet<String> uniqueLinks = new HashSet<String>();
        while (values.hasNext()) {
            uniqueLinks.add(values.next().toString());
        }
        output.collect(key, new Text(String.valueOf(uniqueLinks.size())));
    }
}
