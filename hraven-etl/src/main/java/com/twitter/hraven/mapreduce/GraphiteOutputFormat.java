package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.twitter.common.collections.Pair;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.etl.HadoopUtil;
import com.twitter.hraven.util.EnumWritable;

/**
 * @author angad.singh {@link OutputFormat} for sending metrics to graphite
 */

public class GraphiteOutputFormat extends OutputFormat<EnumWritable<HravenService>, HravenRecord> {

  private static Log LOG = LogFactory.getLog(GraphiteOutputFormat.class);
  private static Map<String, GraphiteSinkConf> sinkConfMap;
  private static Map<String, Pair<Writer,Socket>> graphiteSockets;

  /**
   * {@link OutputCommitter} which does nothing
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
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
    }

  }

  protected static class GraphiteRecordWriter extends RecordWriter<EnumWritable<HravenService>, HravenRecord> {
    private HTable keyMappingTable;
    private HTable reverseKeyMappingTable;
    private TaskAttemptContext context;
    
    public GraphiteRecordWriter(TaskAttemptContext context, Configuration hbaseconfig) throws IOException {
      this.context = context;
      keyMappingTable = new HTable(hbaseconfig, Constants.GRAPHITE_KEY_MAPPING_TABLE_BYTES);
      keyMappingTable.setAutoFlush(false);

      reverseKeyMappingTable = new HTable(hbaseconfig, Constants.GRAPHITE_REVERSE_KEY_MAPPING_TABLE_BYTES);
      reverseKeyMappingTable.setAutoFlush(false);
    }

    /**
     * Split a {@link JobHistoryRecordCollection} into {@link JobHistoryRecord}s and call the
     * {@link #writeRecord(HravenService, JobHistoryRecord)} method
     */

    @Override
    public void write(EnumWritable<HravenService> serviceKey, HravenRecord value) throws IOException,
        InterruptedException {
      HravenService service = serviceKey.getValue();
      JobHistoryRecordCollection recordCollection;

      if (value instanceof JobHistoryRecordCollection) {
        recordCollection = (JobHistoryRecordCollection) value;
      } else {
        recordCollection = new JobHistoryRecordCollection((JobHistoryRecord) value);
      }
      
      for (Entry<String, GraphiteSinkConf> confEntry : sinkConfMap.entrySet()) {
        try{
            GraphiteHistoryWriter graphiteWriter =
                    new GraphiteHistoryWriter(context, keyMappingTable,
                            reverseKeyMappingTable, service, recordCollection,
                            confEntry.getValue(), graphiteSockets.get(confEntry.getKey()).getFirst());
            graphiteWriter.write();
        } catch (Exception e) {
            LOG.error("Error generating metrics for graphite", e);
            throw new IOException(e);
        }
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      try {
        LOG.info("flushing records and closing writers");
        for (Entry<String, Pair<Writer, Socket>> writerEntry: graphiteSockets.entrySet()) {
            writerEntry.getValue().getFirst().close();
            writerEntry.getValue().getSecond().close();
        }
      } catch (Exception e) {
        throw new IOException("Error flush metrics to graphite", e);
      }
      keyMappingTable.close();
      reverseKeyMappingTable.close();
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
  public RecordWriter<EnumWritable<HravenService>, HravenRecord> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    if (sinkConfMap == null) {
        sinkConfMap = new HashMap<String, GraphiteSinkConf>();
        graphiteSockets = new HashMap<String, Pair<Writer,Socket>>();
        String confPath = context.getConfiguration().get(Constants.JOBCONF_GRAPHITE_CONFIG_PATH);
        String[] sinkConfigs = confPath.split(",");
        for (String configFile : sinkConfigs) {
            ObjectMapper objectMapper = new ObjectMapper();
            String configStr = HadoopUtil.readFsFile(configFile, new Configuration());
            GraphiteSinkConf sinkConf = objectMapper.readValue(configStr, GraphiteSinkConf.class);
            sinkConf.setName(Paths.get(configFile).getFileName().toString());
            sinkConfMap.put(configFile, sinkConf);
            try {
                // Open an connection to the graphite server for this sink config
                Socket socket = new Socket(sinkConf.getGraphiteHost(), sinkConf.getGraphitePort());
                graphiteSockets.put(configFile, new Pair<Writer, Socket>(new OutputStreamWriter(socket.getOutputStream()), socket));
              } catch (Exception e) {
                throw new IOException("Error connecting to graphite sink, " + configFile + "(" + sinkConf.getGraphiteHost() + ":" + sinkConf.getGraphitePort() + ")", e);
              }
        }
        LOG.info("Graphite sink config map: " + sinkConfMap.toString());
    }
    
    return new GraphiteRecordWriter(context, HBaseConfiguration.create(conf));
  }

}
