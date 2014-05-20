package com.twitter.hraven;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 
 * @author angad.singh
 *
 * Store multiple {@link JobHistoryRecord}s in a 2 level HashMap
 * 
 * Supports iteration to get individual {@link JobHistoryRecord}s
 */

public class JobHistoryMultiRecord extends HravenRecord<JobKey, Object> implements
    Iterable<JobHistoryRecord> {

  private Map<RecordCategory, Map<RecordDataKey, Object>> valueMap;

  public JobHistoryMultiRecord() {
    valueMap = new HashMap<RecordCategory, Map<RecordDataKey, Object>>();
  }

  public JobHistoryMultiRecord(JobKey jobKey) {
    this.setKey(jobKey);
    valueMap = new HashMap<RecordCategory, Map<RecordDataKey, Object>>();
  }

  public void add(RecordCategory category, RecordDataKey key, Object value) {
    if (valueMap.containsKey(category)) {
      valueMap.get(category).put(key, value);
    } else {
      HashMap<RecordDataKey, Object> categoryMap = new HashMap<RecordDataKey, Object>();
      valueMap.put(category, categoryMap);
      categoryMap.put(key, value);
    }
  }

  public void add(HravenRecord record) {
    add(record.getDataCategory(), record.getDataKey(), record.getDataValue());
  }

  public Map<RecordCategory, Map<RecordDataKey, Object>> getValueMap() {
    return valueMap;
  }

  public Object getValue(RecordCategory category, RecordDataKey key) {
    return valueMap.containsKey(category) ? valueMap.get(category).get(key) : null;
  }

  public int size() {
    int size = 0;
    for (Entry<RecordCategory, Map<RecordDataKey, Object>> catMap : valueMap.entrySet()) {
      size += catMap.getValue().size();
    }

    return size;
  }

  /**
   * Be able to iterate easily to get individual {@link JobHistoryRecord}s
   */
  
  @Override
  public Iterator<JobHistoryRecord> iterator() {

    return new Iterator<JobHistoryRecord>() {

      private Iterator<Entry<RecordCategory, Map<RecordDataKey, Object>>> catIterator;
      private Iterator<Entry<RecordDataKey, Object>> dataIterator;
      Entry<RecordCategory, Map<RecordDataKey, Object>> nextCat;
      Entry<RecordDataKey, Object> nextData;

      {
        initIterators();
      }

      private void initIterators() {
        if (catIterator == null) {
          catIterator = valueMap.entrySet().iterator();  
        }
        
        if (catIterator.hasNext()) {
          nextCat = catIterator.next();
          dataIterator = nextCat.getValue().entrySet().iterator();  
        }
      }
      
      @Override
      public boolean hasNext() {
        if (dataIterator == null) {
          initIterators();
        }        
        return dataIterator == null ? false : dataIterator.hasNext() || catIterator.hasNext();
      }

      @Override
      public JobHistoryRecord next() {
        if (!dataIterator.hasNext()) {
          nextCat = catIterator.next();
          dataIterator = nextCat.getValue().entrySet().iterator();
        }

        nextData = dataIterator.next();

        return new JobHistoryRecord(nextCat.getKey(), getKey(), nextData.getKey(),
            nextData.getValue(), getSubmitTime());
      }

      @Override
      public void remove() {
      }

    };
  }
}