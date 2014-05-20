package com.twitter.hraven;

public class JobHistoryRawRecord extends HravenRecord<String, Object> {

  public JobHistoryRawRecord(RecordCategory dataCategory, String key, RecordDataKey dataKey,
      Object dataValue) {
    this.setKey(key);
    this.setDataCategory(dataCategory);
    this.setDataKey(dataKey);
    this.setDataValue(dataValue);
  }

  public JobHistoryRawRecord() {

  }

  public JobHistoryRawRecord(String taskKey) {
    this.setKey(taskKey);
  }

  public void set(RecordCategory category, RecordDataKey key, String value) {
    this.setDataCategory(category);
    this.setDataKey(key);
    this.setDataValue(value);
  }
}
