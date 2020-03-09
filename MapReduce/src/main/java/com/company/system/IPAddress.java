package com.company.system;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class IPAddress {

    private final static IntWritable ONE = new IntWritable(1);

    public static void main(final String[] args) throws Exception {

        Configuration configuration = new Configuration();

        //Create a new Job
        final Job job = Job.getInstance(configuration, "ipAddress");
        job.setJarByClass(IPAddress.class);

        //Specify various job-specific parameters

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(RequestData.class);

        job.setMapperClass(IPMap.class);
        job.setCombinerClass(IPReduce.class);
        job.setReducerClass(IPReduce.class);

        //Submit the job, then poll for progress until the job is complete
        boolean value = job.waitForCompletion(true);

        for(CounterGroup group : job.getCounters()) {
            for(Counter counter : group) {
                System.out.println(counter.getName() + " " + counter.getValue());
            }
        }

        if(job.isSuccessful()) {
            System.out.println("Success");
        } else if(!job.isSuccessful()) {
            System.out.println("Error");
        }

        System.out.print("IP value: " + value + "\n");
    }

    public static final class IPMap extends Mapper<LongWritable, Text, IntWritable, RequestData> {

        @Override
        public final void map(final LongWritable key, final Text value, final Context context) {

            RequestData requestData = new RequestData(value.toString());

            ONE.set((int) requestData.getId().get());
            UserAgent userAgent = new UserAgent(value.toString());

            try {
                context.getCounter("Browser: ", userAgent.getBrowser().getName()).increment(1);
                context.write(ONE, requestData);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static final class IPReduce extends Reducer<IntWritable, RequestData, IntWritable, RequestData> {

        @Override
        public final void reduce(final IntWritable key, final Iterable<RequestData> values, final Context context)
                throws IOException, InterruptedException {
            RequestData requestData = new RequestData();

            for (RequestData val : values) {
                requestData.addToSum(val.getSum().get());
            }

            requestData.setId(new LongWritable(key.get()));
            context.write(key, requestData);
        }
    }

    public static final class RequestData implements Writable {

        private LongWritable id;
        private LongWritable count;
        private LongWritable sum;

        public RequestData() {
            id = new LongWritable(0);
            count = new LongWritable(0);
            sum = new LongWritable(0);
        }

        public RequestData(String data) {
            String[] dates = data.split(" ");
            this.id = new LongWritable(Long.parseLong(dates[0].substring(2)));
            this.sum = new LongWritable(Long.parseLong(dates[9]));
            count = new LongWritable(0);
        }

        //Serialize the fields of this object to out
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            id.write(dataOutput);
            count.write(dataOutput);
            sum.write(dataOutput);
        }

        //Deserialize the fields of this object from in/
        @Override
        public void readFields(DataInput dataInput) throws IOException {
            id.readFields(dataInput);
            count.readFields(dataInput);
            sum.readFields(dataInput);
        }

        public void addToSum(long add) {
            sum.set(sum.get() + add);
            count.set(count.get() + 1);
        }

        public LongWritable getId() {
            return id;
        }

        public void setId(LongWritable id) {
            this.id = id;
        }

        public LongWritable getSum() {
            return sum;
        }

        public void setSum(LongWritable sum) { this.sum = sum; }

        public void setCount(LongWritable count) { this.count = count; }
    }
}