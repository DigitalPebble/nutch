/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.tools.warc;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapred.WARCInputFormat;
import com.martinkl.warc.mapred.WARCOutputFormat;

/**
 * MapReduce job to import WARC files into Nutch segments. Simulates the output
 * of a fetch step.
 **/

public class WARCImporter extends Configured implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(WARCImporter.class);

  // TODO write a mapper
  
  public int importWARC(String output, List<Path> segments) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("WARCimporter: starting at {}", sdf.format(start));

    final JobConf job = new NutchJob(getConf());
    job.setJobName("warc-importer " + output);

    for (final Path segment : segments) {
      LOG.info("warc-importer: adding segment: " + segment);
      FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
    }

    job.setInputFormat(WARCInputFormat.class);

    // job.setMapperClass(WARCReducer.class);
    // job.setReducerClass(WARCReducer.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(WARCWritable.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(WARCWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    // using the old api
    job.setOutputFormat(WARCOutputFormat.class);

    try {
      RunningJob rj = JobClient.runJob(job);
      LOG.info(rj.getCounters().toString());
      long end = System.currentTimeMillis();
      LOG.info("WARCimporter: finished at {}, elapsed: {}", sdf.format(end),
          TimingUtil.elapsedTime(start, end));
    } catch (Exception e) {
      LOG.error("Exception caught", e);
      return -1;
    }

    return 0;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: WARCImporter <input> <segmentDir>");
      return -1;
    }

    String input = args[0];
    String segmentDir = args[1];

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("WARCimporter: starting at {}", sdf.format(start));

    final JobConf job = new NutchJob(getConf());
    job.setJobName("warc-importer " + input + " > " + segmentDir);

    // TODO generate new segment in segment dir

    job.setInputFormat(WARCInputFormat.class);

    // job.setMapperClass(WARCReducer.class);
    // job.setReducerClass(WARCReducer.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(WARCWritable.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(WARCWritable.class);

    // TODO set output to segment

    // FileOutputFormat.setOutputPath(job, new Path(output));

    // using the old api
    job.setOutputFormat(WARCOutputFormat.class);

    try {
      RunningJob rj = JobClient.runJob(job);
      LOG.info(rj.getCounters().toString());
      long end = System.currentTimeMillis();
      LOG.info("WARCimporter: finished at {}, elapsed: {}", sdf.format(end),
          TimingUtil.elapsedTime(start, end));
    } catch (Exception e) {
      LOG.error("Exception caught", e);
      return -1;
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new WARCImporter(), args);
    System.exit(res);
  }
}
