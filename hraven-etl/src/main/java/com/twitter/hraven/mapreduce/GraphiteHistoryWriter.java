package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.hraven.Constants;
import com.twitter.hraven.Framework;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobHistoryMultiRecord;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;

public class GraphiteHistoryWriter {

  private static Log LOG = LogFactory.getLog(GraphiteHistoryWriter.class);

  private final Pattern APPID_PATTERN_OOZIE_LAUNCHER = Pattern
      .compile("oozie:launcher:T=(.*):W=(.*):A=(.*):ID=(.*)");
  private final Pattern APPID_PATTERN_PIGJOB = Pattern.compile("PigLatin:(.*).pig");
  private static final Pattern GRAPHITE_KEY_FILTER = Pattern.compile("[./\\\\\\-\\s]+");
  private static final int PIG_ALIAS_FINGERPRINT_LENGTH = 100;

  private HravenService service;
  private JobHistoryMultiRecord records;
  private String PREFIX;

  /**
   * Writes a single {@link JobHistoryRecord} to the specified {@link HravenService} Passes the
   * large multi record of which this record is a part of, so that we can get other contextual
   * attributes to use in the graphite metric naming scheme
   * @param serviceKey
   * @param jobRecord
   * @param multiRecord
   * @throws IOException
   * @throws InterruptedException
   */

  public GraphiteHistoryWriter(String prefix, HravenService serviceKey,
      JobHistoryMultiRecord records) {
    this.service = serviceKey;
    this.records = records;
    this.PREFIX = prefix;
  }

  public String getOutput() throws IOException {
    StringBuilder lines = new StringBuilder();

    for (JobHistoryRecord jobRecord : records) {
      if (service == HravenService.JOB_HISTORY
          && jobRecord.getDataCategory() == RecordCategory.HISTORY_COUNTER) {

        /*
         * Send metrics in the format {PREFIX}.{cluster}.{user}.{appId}.{subAppId} {value}
         * {submit_time} subAppId is formed differently for each framework For pig, it is the alias
         * names of the job appId will be parsed with a bunch of known patterns (oozie launcher
         * jobs, pig jobs, etc.)
         */

        Framework framework = getFramework(records);
        StringBuilder metricsPathPrefix;

        String pigAliasFp = getPigAliasFingerprint(records);

        if (framework == Framework.PIG && pigAliasFp != null) {
          // TODO: should ideally include app version too, but PIG-2587's pig.logical.plan.signature
          // which hravne uses was available only from pig 0.11
          metricsPathPrefix =
              generatePathPrefix(PREFIX, jobRecord.getKey().getCluster(), jobRecord.getKey()
                  .getUserName(), genAppId(records, jobRecord.getKey().getAppId()), pigAliasFp);
        } else {
          metricsPathPrefix =
              generatePathPrefix(PREFIX, jobRecord.getKey().getCluster(), jobRecord.getKey()
                  .getUserName(), genAppId(records, jobRecord.getKey().getAppId()));
        }

        lines.append(metricsPathPrefix.toString());

        for (String comp : jobRecord.getDataKey().getComponents()) {
          lines.append(".").append(sanitize(comp));
        }

        // Round the timestamp to second as Graphite accepts it in such
        // a format.
        int timestamp = Math.round(jobRecord.getSubmitTime() / 1000);
        lines.append(" ").append(jobRecord.getDataValue()).append(" ").append(timestamp)
            .append("\n");
      }
    }

    return lines.toString();
  }

  private String getPigAliasFingerprint(JobHistoryMultiRecord records) {
    Object aliasRec = records.getValue(RecordCategory.CONF, new RecordDataKey("pig.alias"));
    Object featureRec = records.getValue(RecordCategory.CONF, new RecordDataKey("pig.job.feature"));

    String alias = null;
    String feature = null;

    if (aliasRec != null) {
      alias = (String) aliasRec;
    }

    if (featureRec != null) {
      feature = (String) featureRec;
    }

    if (alias != null) {
      return (feature != null ? feature + ":" : "")
          + StringUtils.abbreviate(alias, PIG_ALIAS_FINGERPRINT_LENGTH);
    }

    return null;
  }

  private String genAppId(JobHistoryMultiRecord records, String appId) {
    String oozieActionName = getOozieActionName(records);

    if (getFramework(records) == Framework.PIG && APPID_PATTERN_PIGJOB.matcher(appId).matches()) {
      // pig:{oozie-action-name}:{pigscript}
      if (oozieActionName != null) appId =
          APPID_PATTERN_PIGJOB.matcher(appId).replaceAll("pig:" + oozieActionName + ":$1");
      else appId = APPID_PATTERN_PIGJOB.matcher(appId).replaceAll("pig:$1");

    } else if (APPID_PATTERN_OOZIE_LAUNCHER.matcher(appId).matches()) {
      // ozl:{oozie-workflow-name}
      appId = APPID_PATTERN_OOZIE_LAUNCHER.matcher(appId).replaceAll("ozl:$2");
    } else {
      if (oozieActionName != null) {
        return oozieActionName + ":" + appId;
      }
    }

    return appId;
  }

  private Framework getFramework(JobHistoryMultiRecord records) {
    Object rec =
        records.getValue(RecordCategory.CONF_META, new RecordDataKey(Constants.FRAMEWORK_COLUMN));

    if (rec != null) {
      return Framework.valueOf((String) rec);
    }

    return null;
  }

  private String getOozieActionName(JobHistoryMultiRecord records) {
    Object rec = records.getValue(RecordCategory.CONF, new RecordDataKey("oozie.action.id"));

    if (rec != null) {
      String actionId = ((String) rec);
      return actionId.substring(actionId.indexOf("@") + 1, actionId.length());
    }

    return null;
  }

  /**
   * Util method to generate metrix path prefix
   * @return
   * @throws UnsupportedEncodingException
   */

  private StringBuilder generatePathPrefix(String... args) {
    StringBuilder prefix = new StringBuilder();
    boolean first = true;
    for (String arg : args) {
      if (!first) {
        prefix.append(".");
      }

      prefix.append(sanitize(arg));
      first = false;
    }
    return prefix;
  }

  /**
   * Util method to sanitize metrics for sending to graphite E.g. remove periods ("."), etc.
   * @throws UnsupportedEncodingException
   */
  private String sanitize(String s) {
    return GRAPHITE_KEY_FILTER.matcher(s).replaceAll("_");
  }

  /**
   * Output the {@link JobHistoryRecord} received in debug log
   * @param serviceKey
   * @param jobRecord
   */

  public static void logRecord(HravenService serviceKey, JobHistoryRecord jobRecord) {
    StringBuilder line = new StringBuilder();
    String seperator = ", ";
    String seperator2 = "|";

    line.append("Service: " + serviceKey.name());

    JobKey key = jobRecord.getKey();
    line.append(seperator).append("Cluster: " + key.getCluster());
    line.append(seperator).append("User: " + key.getUserName());
    line.append(seperator).append("AppId: " + key.getAppId());
    line.append(seperator).append("RunId: " + key.getRunId());
    line.append(seperator).append("JobId: " + key.getJobId());

    line.append(seperator2);
    line.append(seperator).append("Category: " + jobRecord.getDataCategory().name());

    line.append(seperator).append("Key: ");
    for (String comp : jobRecord.getDataKey().getComponents()) {
      line.append(comp).append(".");
    }

    line.append(seperator).append("Value: " + jobRecord.getDataValue());
    line.append(seperator).append("SubmitTime: " + jobRecord.getSubmitTime());

    LOG.debug(line);
  }
}
