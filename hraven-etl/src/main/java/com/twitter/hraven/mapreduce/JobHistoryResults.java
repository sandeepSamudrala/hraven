package com.twitter.hraven.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;

import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobHistoryMultiRecord;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobKey;

public class JobHistoryResults {

  private JobDesc jobDesc;
  private JobKey jobKey;
  private long submitTime;
  private List<Pair<HravenService, HravenRecord>> sinkList;

  public JobHistoryResults(JobKey jobKey, JobDesc jobDesc) {
    this.setJobKey(jobKey);
    this.setJobDesc(jobDesc);
    this.sinkList = new ArrayList<Pair<HravenService, HravenRecord>>();
  }

  public JobDesc getJobDesc() {
    return jobDesc;
  }

  public void setJobDesc(JobDesc jobDesc) {
    this.jobDesc = jobDesc;
  }

  public JobKey getJobKey() {
    return jobKey;
  }

  public void setJobKey(JobKey jobKey) {
    this.jobKey = jobKey;
  }

  public void setSubmitTime(long submitTimeMillis) {
    this.submitTime = submitTimeMillis;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }

  public void sink(HravenService service, JobHistoryMultiRecord record) {
    this.sinkList.add(new Pair<HravenService, HravenRecord>(service, record));
  }

  public void sink(HravenService service, List<JobHistoryRecord> records) {
    for (JobHistoryRecord record : records) {
      this.sinkList.add(new Pair<HravenService, HravenRecord>(service, record));
    }
  }

  public void sink(HravenService service, JobHistoryRecord record) {
    this.sinkList.add(new Pair<HravenService, HravenRecord>(service, record));
  }
}
