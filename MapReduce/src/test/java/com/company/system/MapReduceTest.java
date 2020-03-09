package com.company.system;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest
{

    private ReduceDriver<IntWritable, IPAddress.RequestData, IntWritable, IPAddress.RequestData> reduceDriver;
    private MapDriver<LongWritable, Text, IntWritable, IPAddress.RequestData> mapDriver;

    @Test
    public void shouldAnswerWithTrueMap() throws IOException {

        IPAddress.RequestData requestData = new IPAddress.RequestData();
        IPAddress.RequestData requestData1 = new IPAddress.RequestData();

        mapDriver.withInput(new LongWritable(1), new Text("ip1 - Google Chrome"));
        mapDriver.withOutput(new IntWritable((int)requestData.getId().get()), requestData);
        mapDriver.withOutput(new IntWritable((int)requestData1.getId().get()), requestData1);
        reduceDriver.runTest();
    }

    @Test
    public void shouldAnswerWithTrueReduce() throws IOException {
        List<IPAddress.RequestData> list = new ArrayList();
        list.add(new IPAddress.RequestData());
        list.add(new IPAddress.RequestData());
        list.get(0).setId(new LongWritable(1));
        list.get(0).setSum(new LongWritable(25000));
        list.get(1).setId(new LongWritable(1));
        list.get(1).setSum(new LongWritable(12500));

        IPAddress.RequestData requestData = new IPAddress.RequestData();
        requestData.setId(new LongWritable(1));
        requestData.setSum(new LongWritable(50000));
        requestData.setCount(new LongWritable(5));

        reduceDriver.withInput(new IntWritable(1), list);
        reduceDriver.withOutput(new IntWritable(1), requestData);
        reduceDriver.runTest();
    }
}