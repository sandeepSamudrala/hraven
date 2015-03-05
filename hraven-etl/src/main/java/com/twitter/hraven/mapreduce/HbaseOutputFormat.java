package com.twitter.hraven.mapreduce;

import com.twitter.hraven.*;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.TaskKeyConverter;
import com.twitter.hraven.util.EnumWritable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * @author angad.singh Wrapper around Hbase's {@link MultiTableOutputFormat} Converts
 *         {@link HravenRecords} to Hbase {@link Put}s and writes them to {@link HTable}s
 *         corresponding to {@link HravenService}
 */

public class HbaseOutputFormat extends OutputFormat<EnumWritable<HravenService>, HravenRecord> {

  protected static class HravenHbaseRecordWriter extends RecordWriter<EnumWritable<HravenService>, HravenRecord> {

    private RecordWriter<ImmutableBytesWritable, Mutation> recordWriter;

    public HravenHbaseRecordWriter(RecordWriter<ImmutableBytesWritable, Mutation> recordWriter) {
      this.recordWriter = recordWriter;
    }

    /**
     * Writes a single {@link HravenRecord} to the specified {@link HravenService}
     *
     * @param serviceKey
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeRecord(HravenService serviceKey, HravenRecord value)
        throws IOException, InterruptedException {
      ImmutableBytesWritable table = null;
      Put put = null;

      switch (serviceKey) {
      case JOB_HISTORY:
        JobHistoryRecord rec = (JobHistoryRecord) value;
        put = new Put(new JobKeyConverter().toBytes(rec.getKey()));
        table = new ImmutableBytesWritable(Constants.HISTORY_TABLE_BYTES);
        HbaseHistoryWriter.addHistoryPuts(rec, put);
        break;
      case JOB_HISTORY_TASK:
        JobHistoryTaskRecord taskRec = (JobHistoryTaskRecord) value;
        put = new Put(new TaskKeyConverter().toBytes((TaskKey) taskRec.getKey()));
        table = new ImmutableBytesWritable(Constants.HISTORY_TASK_TABLE_BYTES);
        HbaseHistoryWriter.addHistoryPuts(taskRec, put);
        break;
      }

      recordWriter.write(table, put);
    }

    /**
     * Split a {@link JobHistoryRecordCollection} into {@link JobHistoryRecord}s and call the
     * {@link #writeRecord(HravenService, JobHistoryRecord)} method
     */

    @Override
    public void write(EnumWritable<HravenService> serviceKey, HravenRecord value) throws IOException,
        InterruptedException {
      HravenService service = serviceKey.getValue();
      if (value instanceof JobHistoryRecordCollection) {
        for (JobHistoryRecord record : (JobHistoryRecordCollection) value) {
          writeRecord(service, record);
        }
      } else {
        writeRecord(service, value);
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
  public RecordWriter<EnumWritable<HravenService>, HravenRecord> getRecordWriter(TaskAttemptContext context)
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
