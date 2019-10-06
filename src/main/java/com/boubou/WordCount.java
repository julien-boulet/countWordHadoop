package com.boubou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {

    /**
     * Mapper class to split part of text, given to this worker, in word. Write in context each word with 1
     * Used by job1
     */
    private static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException {
            conf = context.getConfiguration();
            // set case sensitive to true by default. To activate it set this variable to false when you start program
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line.toLowerCase(), " \t\n\r\f\"[]'.,/#!$%^&*;:{}=-_`~()«»?");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer class to count words. Write in context the word and its count in this worker
     * Used by Job1
     */
    private static class SumReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Mapper class to switch key and value to by able to decreasing order the result by word count
     * Used by the job2
     */
    private static class KeyValueSwappingMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    /**
     * Reducer to limit the number of result to 100
     * Use by the job2
     */
    private static class LimitReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

        // Works because I force only one reducer on Job2
        private int count = 0;

        private int maxResults;
        private Configuration conf;

        @Override
        public void setup(Reducer.Context context) throws IOException {
            conf = context.getConfiguration();
            // set the number of results to 100 by default.
            maxResults = conf.getInt("wordcount.nb.results", 100);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator i$ = values.iterator();

            while(i$.hasNext() && count < maxResults) {
                Text value = (Text) i$.next();
                context.write(key, value);
                count++;
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out> ");
            System.exit(2);
        }

        Path inputPath = new Path(remainingArgs[0]);
        Path outputDir = new Path(remainingArgs[1]);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) hdfs.delete(outputDir, true);

        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        // to check if it word with many reducers
        job1.setNumReduceTasks(5);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, new Path(outputDir, "out1"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "sort by frequency");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(KeyValueSwappingMapper.class);
        // force only ONE reducer because I don't know how to make a shared count variable between each reducers
        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setCombinerClass(LimitReducer.class);
        job2.setReducerClass(LimitReducer.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(outputDir, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(outputDir, "out2"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
