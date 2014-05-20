package com.twitter.hraven;

/**
 * 
 * @author angad.singh
 * 
 * Abstraction of a record to be stored in the {@link HravenService#JOB_HISTORY} service.
 * Was earlier directly written as an Hbase put
 */

public class JobHistoryRecord extends HravenRecord<JobKey, Object> {

  public JobHistoryRecord(RecordCategory dataCategory, JobKey key, RecordDataKey dataKey,
      Object dataValue) {
    this.setKey(key);
    this.setDataCategory(dataCategory);
    this.setDataKey(dataKey);
    this.setDataValue(dataValue);
  }

  public JobHistoryRecord(RecordCategory dataCategory, JobKey key, RecordDataKey dataKey,
      Object dataValue, long submitTime) {
    this(dataCategory, key, dataKey, dataValue);
    setSubmitTime(submitTime);
  }

  public JobHistoryRecord() {

  }

  public JobHistoryRecord(JobKey jobKey) {
    this.setKey(jobKey);
  }

  public void set(RecordCategory category, RecordDataKey key, String value) {
    this.setDataCategory(category);
    this.setDataKey(key);
    this.setDataValue(value);
  }
}
