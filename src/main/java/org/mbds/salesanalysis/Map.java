/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.mbds.salesanalysis;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author robsona
 */
public class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        String type = configuration.get("etat");
        String data = configuration.get("value");

        if (key.get() > 0) {
            String[] datas = value.toString().split(",");

            String toCalculate;
            float profit = Float.parseFloat(datas[13]);

            switch (type) {
                case "region":
                    toCalculate = datas[0];
                    break;
                case "country":
                    toCalculate = datas[1];
                    break;
                case "item":
                    toCalculate = datas[2];
                    break;
                default:
                    throw new AssertionError();
            }

            if (toCalculate.equalsIgnoreCase(data)) {
                context.write(new Text(toCalculate), new FloatWritable(profit));
            }
        }
    }
}
