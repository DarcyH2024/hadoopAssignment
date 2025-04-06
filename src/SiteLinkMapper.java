/**
 * Class Name: SiteLinkMapper
 * Author: Darcy Harper
 * version 1.0
 * Course: ITEC 3170
 * Written: April 5, 2025
 * The purpose of this class is to implement the Mapper function
 * for a Hadoop MapReduce job that processes web link relationships.
 * It reads each line from the input file, extracts the website ID
 * and its corresponding linked site, and outputs key-value pairs.
 * These pairs will later be processed by the Reducer to count
 * unique links for each website.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SiteLinkMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
        String line = value.toString();
        String [] data = line.split("\\s+");
        String site = data[0].trim();
        String link = data[1].trim();
        output.collect(new Text(site), new Text(link));
    }
}
