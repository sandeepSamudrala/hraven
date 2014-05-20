package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.hraven.Constants;
import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobHistoryMultiRecord;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;

/**
 * 
 * @author angad.singh
 *
 * {@link OutputFormat} for sending metrics to graphite
 * 
 */

public class GraphiteOutputFormat extends OutputFormat<HravenService, HravenRecord> {

  private static Log LOG = LogFactory.getLog(GraphiteOutputFormat.class);

  private static Writer writer;

  /**
   * {@link OutputCommitter} required to flush the writer
   */
  protected static class GraphiteOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      writer.flush();
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
    }

  }

  protected static class GraphiteRecordWriter extends RecordWriter<HravenService, HravenRecord> {

    private static final Log LOG = LogFactory.getLog(GraphiteRecordWriter.class);

    private String host;
    private int port;
    private String METRIC_PREFIX;

    public GraphiteRecordWriter(String host, int port, String prefix) throws IOException {
      this.host = host;
      this.port = port;
      this.METRIC_PREFIX = prefix;

      try {
        // Open an connection to Graphite server.
        Socket socket = new Socket(host, port);
        writer = new OutputStreamWriter(socket.getOutputStream());
      } catch (Exception e) {
        throw new IOException("Error connecting to graphite, " + host + ":" + port, e);
      }
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
      try {
        writer.close();
      } catch (Exception e) {
        throw new IOException("Error flush metrics to graphite", e);
      }
    }

    /**
     * Writes a single {@link JobHistoryRecord} to the specified {@link HravenService}
     * 
     * @param serviceKey
     * @param jobRecord
     * @throws IOException
     * @throws InterruptedException
     */
    
    private void writeRecord(HravenService serviceKey, JobHistoryRecord jobRecord)
        throws IOException, InterruptedException {

      logRecord(serviceKey, jobRecord);

      if (serviceKey == HravenService.JOB_HISTORY
          && jobRecord.getDataCategory() == RecordCategory.HISTORY_COUNTER) {

        /*
         * Send metrics in the format {PREFIX}.{cluster}.{user}.{appId}.{runId}.{jobId} value
         * {submit_time}
         */

        StringBuilder metricsPathPrefix = new StringBuilder();

        metricsPathPrefix.append(METRIC_PREFIX).append(".").append(jobRecord.getKey().getCluster())
            .append(jobRecord.getKey().getUserName()).append(jobRecord.getKey().getAppId())
            .append(jobRecord.getKey().getJobId());

        StringBuilder lines = new StringBuilder();

        // Round the timestamp to second as Graphite accepts it in such
        // a format.
        int timestamp = Math.round(jobRecord.getSubmitTime() / 1000);

        // Collect datapoints.
        lines.append(metricsPathPrefix.toString());

        for (String comp : jobRecord.getDataKey().getComponents()) {
          lines.append(".").append(comp);
        }

        lines.append(" ").append(jobRecord.getDataValue()).append(" ").append(timestamp)
            .append("\n");

        try {
          writer.write(lines.toString());
        } catch (Exception e) {
          throw new IOException("Error sending metrics", e);
        }
      }
    }

    /**
     * Output the {@link JobHistoryRecord} received in debug log
     * @param serviceKey
     * @param jobRecord
     */
    
    private void logRecord(HravenService serviceKey, JobHistoryRecord jobRecord) {
      StringBuilder line = new StringBuilder();
      String seperator = ", ";
      String seperator2 = "|";

      line.append("Service: " + serviceKey.name());

      JobKey key = jobRecord.getKey();
      line.append(seperator).append("Cluster: " + key.getCluster());
      line.append(seperator).append("User: " + key.getUserName());
      line.append(seperator).append("AppId: " + key.getAppId());
      line.append(seperator).append("RunId: " + key.getRunId());
      line.append(seperator).append("JobId: " + key.getJobId());

      line.append(seperator2);
      line.append(seperator).append("Category: " + jobRecord.getDataCategory().name());

      line.append(seperator).append("Key: ");
      for (String comp : jobRecord.getDataKey().getComponents()) {
        line.append(comp).append(".");
      }

      line.append(seperator).append("Value: " + jobRecord.getDataValue());
      line.append(seperator).append("SubmitTime: " + jobRecord.getSubmitTime());

      LOG.info(line);
    }

    /**
     * Split a {@link JobHistoryMultiRecord} into {@link JobHistoryRecord}s
     * and call the {@link #writeRecord(HravenService, JobHistoryRecord)} method
     */
    
    @Override
    public void write(HravenService serviceKey, HravenRecord value) throws IOException,
        InterruptedException {
      if (value instanceof JobHistoryMultiRecord) {
        for (JobHistoryRecord record : (JobHistoryMultiRecord) value) {
          writeRecord(serviceKey, record);
        }
      } else {
        writeRecord(serviceKey, (JobHistoryRecord) value);
      }
    }
  }

  @Override
  public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new GraphiteOutputCommitter();
  }

  /**
   * Output a custom {@link GraphiteRecordWriter} to send metrics to graphite
   */
  @Override
  public RecordWriter<HravenService, HravenRecord> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new GraphiteRecordWriter(conf.get(Constants.JOBCONF_GRAPHITE_HOST_KEY), conf.getInt(
      Constants.JOBCONF_GRAPHITE_PORT_KEY, Constants.GRAPHITE_DEFAULT_PORT),
        conf.get(Constants.JOBCONF_GRAPHITE_PREFIX));
  }

}
