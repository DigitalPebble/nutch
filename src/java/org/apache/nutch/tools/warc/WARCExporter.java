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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapred.WARCOutputFormat;

/**
 * MapReduce job to exports Nutch segments as WARC files.
 **/

public class WARCExporter extends Configured implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(WARCExporter.class);

  private static final String CRLF = "\r\n";
  private static final byte[] CRLF_BYTES = { 13, 10 };

  public WARCExporter() {
    super(null);
  }

  public WARCExporter(Configuration conf) {
    super(conf);
  }

  public static class WARCReducer
      implements Mapper<Text, Writable, Text, NutchWritable>,
      Reducer<Text, NutchWritable, NullWritable, WARCWritable> {

    @Override
    public void configure(JobConf job) {
      // TODO anything to configure?
    }

    @Override
    public void close() throws IOException {
    }

    public void map(Text key, Writable value,
        OutputCollector<Text, NutchWritable> output, Reporter reporter)
            throws IOException {
      output.collect(key, new NutchWritable(value));
    }

    @Override
    public void reduce(Text key, Iterator<NutchWritable> values,
        OutputCollector<NullWritable, WARCWritable> output, Reporter reporter)
            throws IOException {

      Content content = null;
      CrawlDatum cd = null;

      // aggregate the values found
      while (values.hasNext()) {
        final Writable value = values.next().get(); // unwrap
        if (value instanceof Content) {
          content = (Content) value;
          continue;
        }
        if (value instanceof CrawlDatum) {
          cd = (CrawlDatum) value;
          continue;
        }
      }

      // check that we have everything we need
      if (content == null) {
        LOG.info("Missing content for {}", key);
        reporter.getCounter("WARCExporter", "missing content").increment(1);
        return;
      }

      if (cd == null) {
        LOG.info("Missing fetch datum for {}", key);
        reporter.getCounter("WARCExporter", "missing metadata").increment(1);
        return;
      }

      // TODO generate a string representation of the headers
      StringBuffer buf = new StringBuffer();
      buf.append(WARCRecord.WARC_VERSION);
      buf.append(CRLF);

      // TODO select what we want to put there
      // e.g. mandatory WARC metadata
      // WARC-Type, WARC-Date,
      // WARC-Target-URI: http://news.bbc.co.uk/2/hi/africa/3414345.stm

      // WARC-Type: warcinfo
      // WARC-Date: 2010-10-08T07:00:26Z
      // WARC-Filename:
      // LOC-MONTHLY-014-20101008070022-00127-crawling111.us.archive.org.warc.gz
      // WARC-Record-ID: <urn:uuid:05de9500-7047-4206-aa7f-346a0dc91b1f>
      // Content-Type: application/warc-fields

      // see
      // https://github.com/iipc/webarchive-commons/blob/master/src/main/java/org/archive/format/warc/WARCRecordWriter.java

      int contentLength = 0;
      if (content != null) {
        contentLength = content.getContent().length;
      }

      buf.append("Content-Length").append(": ")
          .append(Integer.toString(contentLength)).append(CRLF);

      buf.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
          .append(UUID.randomUUID().toString()).append(">").append(CRLF);

      // finished writing the headers, now let's serialize it

      ByteArrayOutputStream bos = new ByteArrayOutputStream();

      // store the headers
      bos.write(buf.toString().getBytes("UTF-8"));
      bos.write(CRLF_BYTES);
      // the binary content itself
      if (content.getContent() != null) {
        bos.write(content.getContent());
      }
      bos.write(CRLF_BYTES);
      bos.write(CRLF_BYTES);

      try {
        DataInput in = new DataInputStream(
            new ByteArrayInputStream(bos.toByteArray()));
        WARCRecord record = new WARCRecord(in);
        output.collect(NullWritable.get(), new WARCWritable(record));
        reporter.getCounter("WARCExporter", "records generated").increment(1);
      } catch (IOException exception) {
        LOG.info("Exception when generating WARC record for {} : {}", key,
            exception.getMessage());
        reporter.getCounter("WARCExporter", "exception").increment(1);
      }

    }
  }

  public int generateWARC(String output, List<Path> segments) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("WARCExporter: starting at {}", sdf.format(start));

    final JobConf job = new NutchJob(getConf());
    job.setJobName("warc-exporter " + output);

    for (final Path segment : segments) {
      LOG.info("warc-exporter: adding segment: " + segment);
      FileInputFormat.addInputPath(job,
          new Path(segment, CrawlDatum.FETCH_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
    }

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(WARCReducer.class);
    job.setReducerClass(WARCReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NutchWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(output));
    // using the old api
    job.setOutputFormat(WARCOutputFormat.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(WARCWritable.class);

    try {
      RunningJob rj = JobClient.runJob(job);
      LOG.info(rj.getCounters().toString());
      long end = System.currentTimeMillis();
      LOG.info("WARCExporter: finished at {}, elapsed: {}", sdf.format(end),
          TimingUtil.elapsedTime(start, end));
    } catch (Exception e) {
      LOG.error("Exception caught", e);
      return -1;
    }

    return 0;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(
          "Usage: WARCExporter <output> (<segment> ... | -dir <segments>)");
      return -1;
    }

    final List<Path> segments = new ArrayList<Path>();

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        Path dir = new Path(args[++i]);
        FileSystem fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] files = HadoopFSUtil.getPaths(fstats);
        for (Path p : files) {
          segments.add(p);
        }
      } else {
        segments.add(new Path(args[i]));
      }
    }

    return generateWARC(args[0], segments);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new WARCExporter(), args);
    System.exit(res);
  }
}
