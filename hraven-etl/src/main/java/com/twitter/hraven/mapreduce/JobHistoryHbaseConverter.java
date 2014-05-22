package com.twitter.hraven.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.RecordCategory;

public class JobHistoryHbaseConverter {
  public static void addHistoryPuts(HravenRecord record, Put p) {
    byte[] family = Constants.INFO_FAM_BYTES;

    JobHistoryKeys key = null;
    if (record.getDataKey() != null && record.getDataKey().get(0) != null) key =
        JobHistoryKeys.valueOf(record.getDataKey().get(0));

    if (key == null) {
      // some keys other than JobHistoryKeys were added by
      // JobHistoryListener
      byte[] qualifier = null;

      if (record.getDataCategory() == RecordCategory.CONF
          || record.getDataKey().get(0).equals(Constants.HRAVEN_QUEUE)) {
        byte[] jobConfColumnPrefix =
            Bytes.toBytes(Constants.JOB_CONF_COLUMN_PREFIX + Constants.SEP_BYTES);
        qualifier = Bytes.add(jobConfColumnPrefix, Bytes.toBytes(key.toString()));
      } else {
        qualifier = Bytes.toBytes(key.toString().toLowerCase());
      }

      byte[] valueBytes = Bytes.toBytes((String) record.getDataValue());
      p.add(family, qualifier, valueBytes);
    } else if (key == JobHistoryKeys.COUNTERS || key == JobHistoryKeys.MAP_COUNTERS
        || key == JobHistoryKeys.REDUCE_COUNTERS) {

      String group = record.getDataKey().get(1);
      String counterName = record.getDataKey().get(2);
      byte[] counterPrefix = null;

      if (key == JobHistoryKeys.COUNTERS) {
        counterPrefix = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
      } else if (key == JobHistoryKeys.MAP_COUNTERS) {
        counterPrefix = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
      } else if (key == JobHistoryKeys.REDUCE_COUNTERS) {
        counterPrefix =
            Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
      } else {
        throw new IllegalArgumentException("Unknown counter type " + key.toString());
      }

      byte[] groupPrefix = Bytes.add(counterPrefix, Bytes.toBytes(group), Constants.SEP_BYTES);
      byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counterName));

      p.add(family, qualifier, Bytes.toBytes((Long) record.getDataValue()));
    } else {
      @SuppressWarnings("rawtypes")
      Class clazz = JobHistoryKeys.KEY_TYPES.get(key);
      byte[] valueBytes = null;

      if (Integer.class.equals(clazz)) {
        valueBytes =
            (Integer) record.getDataValue() == 0 ? Constants.ZERO_INT_BYTES : Bytes
                .toBytes((Integer) record.getDataValue());
      } else if (Long.class.equals(clazz)) {
        valueBytes =
            (Long) record.getDataValue() == 0 ? Constants.ZERO_LONG_BYTES : Bytes
                .toBytes((Long) record.getDataValue());
      } else {
        // keep the string representation by default
        valueBytes = Bytes.toBytes((String) record.getDataValue());
      }
      byte[] qualifier = Bytes.toBytes(key.toString().toLowerCase());
      p.add(family, qualifier, valueBytes);
    }
  }
}
