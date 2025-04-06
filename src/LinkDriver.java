/**
 * Class Name: LinkDriver
 * Author: Darcy Harper
 * Version 1.0
 * Course: ITEC 3170
 * Written: April 5, 2025
 * The purpose of this class is to configure and execute the Hadoop MapReduce job
 * for processing web link relationships. It sets up the Mapper and Reducer classes,
 * defines input and output paths, and runs the job using Hadoop's API.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LinkDriver {
    public static void main(String[] args) throws Exception{
        JobClient client = new JobClient();

        //Create the configuration object for the job
        JobConf jobConf = new JobConf(LinkDriver.class);

        //Set a name for the job
        jobConf.setJobName("UniqueLink");

        //Specify the type of output key and value
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        //Specify names of Mapper and Reducer class
        jobConf.setMapperClass(SiteLinkMapper.class);
        jobConf.setReducerClass(SiteLinkReducer.class);

        //Specify formats of the data type of input and output
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        //Set input and output directories using command line arguments
        //arg[0] = name of input directory on HDFS and arg[1] = name of output directory to be created
        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        client.setConf(jobConf);
        try {
            //run the job
            JobClient.runJob(jobConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
