package org.datasyslab.spatialbenchmark;

import java.io.IOException;
import java.util.StringTokenizer;

import edu.umn.cs.spatialHadoop.OperationsParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hadoop MapReduce takes strings as input and uses key-value pair as the intermediate format between mapper and reducer.
 */
public class SpatialAggregation
{
    /**
     * Mapper:
     * Input: take an input string "shape,shape". The delimiter is TBD.
     * Split the input string to two fields, shape, shape.
     * Use the first shape as the key in key-value pair and append 1 as the count base.
     *
     * Output: generate key-value pair <shape,1>. Shape is just a plain text. 1 is a numerical number.
     *
     * NOTE: No need to parse shape string. Just simply treat it as a key.
     */
    public static class IntSumMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "\t");

            assert itr.countTokens() == 2;
            // Still use the original key shape (aka., left shape of join result <shape, shape>) as the key
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

    /**
     * Reducer:
     * Input: take an input key-value pair from the mapper. <shape,1>
     * Simply adds up all 1 together for each key. In other words, this is a CountByKey.
     *
     * Output: generate key-value pair <shape,count>.
     */
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
        final Path input = params.getInputPath();
        final Path output = params.getOutputPath();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, SpatialAggregation.class.getSimpleName());
        job.setJarByClass(SpatialAggregation.class);
        job.setMapperClass(IntSumMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(input.toString()));
        FileOutputFormat.setOutputPath(job, new Path(output.toString()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
