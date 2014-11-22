import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * ********************************************************************
 * HUBUB CONFIDENTIAL
 * <p>
 * <p>
 * [2013] Hubub Incorporated
 * All Rights Reserved.
 * <p>
 * NOTICE:  All information contained herein is, and remains
 * the property of Hubub Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Hubub Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Hubub Incorporated.
 * **********************************************************************
 */
public class HadoopDriver extends Configured implements Tool {

    @Override
    public int run(String ... args) throws Exception {
        String input, output;
        if (args.length == 2) {
            input = args[0];
            output = args[1];
        } else {
            System.err.println("Incorrect number of arguments.  Expected: input output");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(HadoopDriver.class);
        job.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(EmptySearchReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    /**
     * Meant to be run with a file that has rank and Strings. Won't do much for you
     *
     * i.e.
     * look at the file under src/main/resources for an example
     * @param args
     * @throws Exception
     */
    public static void main(String ... args) throws Exception {
        HadoopDriver driver = new HadoopDriver();
        System.exit(ToolRunner.run(driver, args));
    }


    /*
        Key in, Value In, Key Out, Value Out
     */
    public static class SearchMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Object someThingINeedOnSetup = null;

        @Override
        public void setup(Context context) {
            // do any other setup I need here
        }

        private LongWritable hits;
        private Text url;

        @Override
        public void map(LongWritable number, Text valueIn, Context context) throws IOException, InterruptedException {
            System.out.println(Thread.currentThread().toString() + " processing number " + number + " and string " + valueIn.toString());
            context.write(number, valueIn);
        }
    }

    /*
        Empty implementation of the reducer, not needed here
     */
    public static class EmptySearchReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //TODO this needs to do things like make sure that there is a valid date on the site info ... or in the mapper?

            return;
        }
    }
}
