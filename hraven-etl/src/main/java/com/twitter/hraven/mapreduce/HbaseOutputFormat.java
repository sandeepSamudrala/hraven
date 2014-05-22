package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.Writable;
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
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;
import com.twitter.hraven.TaskKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.TaskKeyConverter;

/**
 * @author angad.singh Wrapper around Hbase's {@link MultiTableOutputFormat} Converts
 *         {@link HravenRecords} to Hbase {@link Put}s and writes them to {@link HTable}s
 *         corresponding to {@link HravenService}
 */

public class HbaseOutputFormat extends OutputFormat<HravenService, HravenRecord> {

  protected static class HravenHbaseRecordWriter extends RecordWriter<HravenService, HravenRecord> {

    private RecordWriter<ImmutableBytesWritable, Writable> recordWriter;

    public HravenHbaseRecordWriter(RecordWriter<ImmutableBytesWritable, Writable> recordWriter) {
      this.recordWriter = recordWriter;
    }

    /**
     * Writes a single {@link JobHistoryRecord} to the specified {@link HravenService}
     * @param serviceKey
     * @param jobRecord
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeRecord(HravenService serviceKey, JobHistoryRecord jobRecord)
        throws IOException, InterruptedException {
      ImmutableBytesWritable table = null;
      Put put = null;

      switch (serviceKey) {
      case JOB_HISTORY:
        put = new Put(new JobKeyConverter().toBytes(jobRecord.getKey()));
        table = new ImmutableBytesWritable(Constants.HISTORY_TABLE_BYTES);
        JobHistoryHbaseConverter.addHistoryPuts(jobRecord, put);
        break;
      case JOB_HISTORY_TASK:
        put = new Put(new TaskKeyConverter().toBytes((TaskKey) jobRecord.getKey()));
        table = new ImmutableBytesWritable(Constants.HISTORY_TABLE_BYTES);
        JobHistoryHbaseConverter.addHistoryPuts(jobRecord, put);
        break;
      }

      recordWriter.write(table, put);
    }

    /**
     * Split a {@link JobHistoryMultiRecord} into {@link JobHistoryRecord}s and call the
     * {@link #writeRecord(HravenService, JobHistoryRecord)} method
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

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      recordWriter.close(context);
    }
  }

  private MultiTableOutputFormat outputFormat;

  /**
   * Wrap around Hbase's {@link MultiTableOutputFormat}
   */
  public HbaseOutputFormat() {
    this.outputFormat = new MultiTableOutputFormat();
  }

  /**
   * Wrap around {@link MultiTableOutputFormat}'s {@link MultiTableRecordWriter}
   */
  @Override
  public RecordWriter<HravenService, HravenRecord> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new HravenHbaseRecordWriter(outputFormat.getRecordWriter(context));
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    outputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    return outputFormat.getOutputCommitter(context);
  }
}
