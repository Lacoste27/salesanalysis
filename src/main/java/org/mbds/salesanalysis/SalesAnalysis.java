/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package org.mbds.salesanalysis;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author robsona
 */
public class SalesAnalysis {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        // Parse command-line arguments.
        String[] ourArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        boolean perregion = false;
        boolean percountry = false;
        boolean peritem = false;

        for (String arg : ourArgs) {
            if (arg.startsWith("-")) {
                String[] parts = arg.substring(1).split("=");

                if (parts.length == 2) {
                    String key = parts[0];
                    String value = parts[1];

                    switch (key) {
                        case "r":
                            perregion = true;
                            configuration.set("etat", "region");
                            configuration.set("value", value);
                            break;
                        case "c":
                            percountry = true;
                            configuration.set("etat", "country");
                            configuration.set("value", value);
                            break;
                        case "i":
                            peritem = true;
                            configuration.set("etat", "item");
                            configuration.set("value", value);
                            break;
                    }

                    break;
                }
            }
        }

        System.out.println(configuration.get("etat"));
        System.out.println(configuration.get("value"));

        Job job = new Job(configuration);
        
        job.setJarByClass(SalesAnalysis.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path("results/salesanalysis/"+configuration.get("etat")+"/"+configuration.get("value")));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
