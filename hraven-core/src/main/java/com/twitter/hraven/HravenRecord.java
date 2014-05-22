package com.twitter.hraven;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;

/**
 * 
 * @author angad.singh
 *
 * {@link JobFileTableMapper outputs this as value. It corresponds to the
 * Put record which was earlier emitted
 * 
 * @param <K> key type
 * @param <V> type of dataValue object to be stored
 */

public abstract class HravenRecord<K, V> {
  private K key;
  private RecordCategory dataCategory;
  private RecordDataKey dataKey;
  private V dataValue;
  private long submitTime;

  public HravenRecord() {

  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public RecordCategory getDataCategory() {
    return dataCategory;
  }

  public void setDataCategory(RecordCategory dataCategory) {
    this.dataCategory = dataCategory;
  }

  public RecordDataKey getDataKey() {
    return dataKey;
  }

  public void setDataKey(RecordDataKey dataKey) {
    this.dataKey = dataKey;
  }

  public V getDataValue() {
    return dataValue;
  }

  public void setDataValue(V dataValue) {
    this.dataValue = dataValue;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }
}
