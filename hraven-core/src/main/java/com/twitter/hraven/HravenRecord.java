package com.twitter.hraven;

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
  private Long submitTime;

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

  public Long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(Long submitTime) {
    this.submitTime = submitTime;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dataCategory == null) ? 0 : dataCategory.hashCode());
    result = prime * result + ((dataKey == null) ? 0 : dataKey.hashCode());
    result = prime * result + ((dataValue == null) ? 0 : dataValue.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + (int) (submitTime ^ (submitTime >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HravenRecord other = (HravenRecord) obj;
    if (dataCategory != other.dataCategory) {
      return false;
    }
    if (dataKey == null) {
      if (other.dataKey != null) {
        return false;
      }
    } else if (!dataKey.equals(other.dataKey)) {
      return false;
    }
    if (dataValue == null) {
      if (other.dataValue != null) {
        return false;
      }
    } else if (!dataValue.equals(other.dataValue)) {
      return false;
    }
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    if (submitTime != other.submitTime) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "HravenRecord [key=" + key + ", dataCategory=" + dataCategory + ", dataKey=" + dataKey
        + ", dataValue=" + dataValue + ", submitTime=" + submitTime + "]";
  }
}
