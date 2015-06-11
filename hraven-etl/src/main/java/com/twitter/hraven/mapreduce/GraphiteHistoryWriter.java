package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.twitter.hraven.Constants;
import com.twitter.hraven.Framework;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.util.ByteUtil;

public class GraphiteHistoryWriter {

  private static Log LOG = LogFactory.getLog(GraphiteHistoryWriter.class);

  private final Pattern APPID_PATTERN_OOZIE_LAUNCHER = Pattern.compile(".*oozie:launcher:T=(.*):W=(.*):A=(.*):ID=(.*)");
  private final Pattern APPID_PATTERN_OOZIE_ACTION = Pattern.compile(".*oozie:action:T=(.*):W=(.*):A=(.*):ID=[0-9]{7}-[0-9]{15}-oozie-oozi-W(.*)");
  private static final String GRAPHITE_KEY_FILTER = "[./\\\\\\s,]";
  private static final int PIG_ALIAS_FINGERPRINT_LENGTH = 100;
  
  private static final String submitTimeKey = JobHistoryKeys.SUBMIT_TIME.toString();
  private static final String launchTimeKey = JobHistoryKeys.LAUNCH_TIME.toString();
  private static final String finishTimeKey = JobHistoryKeys.FINISH_TIME.toString();
  private static final String totalTimeKey = "total_time";
  private static final String runTimeKey = "run_time";

  private HravenService service;
  private JobHistoryRecordCollection recordCollection;
  
  private HTable keyMappingTable;
  private HTable reverseKeyMappingTable;
  private TaskAttemptContext taskContext;
  private GraphiteSinkConf sinkConfig;
  private Writer socketWriter;

  private List<String> userFilter;
  private List<String> queueFilter;
  private List<String> excludedComponents;
  private List<String> appInclusionFilter;
  private List<String> appExclusionFilter;

  
  public enum Counters {
    GRAPHITE_SINK,
    APPS_FILTERED_IN,
    APPS_FILTERED_OUT,
    APP_EXCLUDED_COMPS,
    METRICS_WRITTEN
  };

  /**
   * Writes a single {@link JobHistoryRecord} to the specified {@link HravenService} Passes the
   * large multi record of which this record is a part of, so that we can get other contextual
   * attributes to use in the graphite metric naming scheme
   * @param context
   * @param keyMappingTable
   * @param reverseKeyMappingTable
   * @param serviceKey
   * @param recordCollection
   * @param userFilter 
   * @param queueFilter
   * @param excludedComponents
   * @param appExclusionFilter
   * @param metricNamingRuleFile
   * @throws IOException
   * @throws InterruptedException
   */

  public GraphiteHistoryWriter(TaskAttemptContext context, HTable keyMappingTable, HTable reverseKeyMappingTable, HravenService serviceKey,
      JobHistoryRecordCollection recordCollection, GraphiteSinkConf sinkConfig, Writer socketWriter) {
    this.taskContext = context;
    this.keyMappingTable = keyMappingTable;
    this.reverseKeyMappingTable = reverseKeyMappingTable;
    this.service = serviceKey;
    this.recordCollection = recordCollection;
    this.sinkConfig = sinkConfig;
    this.socketWriter = socketWriter;
    
    LOG.info("Working with metric rule file: " + sinkConfig.getMetricNamingRules().toString());
    
    if (StringUtils.isNotEmpty(sinkConfig.getUserfilter()))
      this.userFilter = Arrays.asList(sinkConfig.getUserfilter().split(","));
    if (StringUtils.isNotEmpty(sinkConfig.getQueuefilter()))
      this.queueFilter = Arrays.asList(sinkConfig.getQueuefilter().split(","));
    if (StringUtils.isNotEmpty(sinkConfig.getExcludedComponents()))
      this.excludedComponents = Arrays.asList(sinkConfig.getExcludedComponents().split(","));
    if (StringUtils.isNotEmpty(sinkConfig.getIncludeApps()))
      this.appInclusionFilter = Arrays.asList(sinkConfig.getIncludeApps().split(","));
    if (StringUtils.isNotEmpty(sinkConfig.getExcludeApps()))
      this.appExclusionFilter = Arrays.asList(sinkConfig.getExcludeApps().split(","));
  }

  private boolean isAppExcluded(String appId) {
    if (this.appExclusionFilter == null)
      return false;
    
    for (String s: appExclusionFilter) {
      if (appId.indexOf(s) != -1)
        return true;
    }
    
    return false;
  }
  
  private boolean isAppIncluded(String appId) {
    if (this.appInclusionFilter == null)
      return true;
    
    for (String s: this.appInclusionFilter) {
      if (appId.indexOf(s) != -1) {
        return true;
      }
    }
    
    return false;
  }
  
  private boolean filterApp() {
      return ( (userFilter == null || userFilter.contains(recordCollection.getKey().getUserName()))
              && (queueFilter == null || queueFilter.contains(
                                              recordCollection.getValue(RecordCategory.CONF_META,
                                              new RecordDataKey(Constants.HRAVEN_QUEUE))))
              ) ||(isAppIncluded(recordCollection.getKey().getAppId())
                   && !isAppExcluded(recordCollection.getKey().getAppId()));
  }
  
  private Map<String, Object> getSystemTokens() {
    return new HashMap<String, Object>() {{
      
        //General tokens
        put("cluster", recordCollection.getKey().getCluster());
        put("queue", getQueue());
        put("user", recordCollection.getKey().getUserName());
        put("jobId", recordCollection.getKey().getJobId().getJobIdString());
        put("encodedRunId", Long.toString(recordCollection.getKey().getEncodedRunId()));
        put("runId", Long.toString(recordCollection.getKey().getRunId()));
        put("jobName", recordCollection.getKey().getAppId());
        put("framework", getFramework().name());
        put("status", getJobStatus());
        put("priority", getJobPriority());
        put("appVersion", getVersion());
        
        //Oozie tokens
        put("oozieActionName", getOozieActionName());
        put("oozieLauncherPattern", getOozieLauncherPattern());
        put("oozieActionPattern", getOozieActionPattern());
        
        //Pig tokens
        put("pigAliasFp", getPigAliasFingerprint());
        put("pigAlias", getPigAlias());
        put("pigFeature", getPigFeature());
        
    }};
  }
  
  private Map<String, String> getUserTags() {
    String confHravenMetricTags = (String)recordCollection.getValue(RecordCategory.CONF, new RecordDataKey(Constants.JOBCONF_HRAVEN_METRIC_TAGS));
    Map<String, String> userTags = new HashMap<String, String>();
    
    if (confHravenMetricTags != null) {
      for (String ele: confHravenMetricTags.split(",")) {
        String[] tag = ele.split("=");
        userTags.put(tag[0], tag[1]);
      }
    }
    
    return userTags;
  }
  
  private Expression getParsedExpression(String exp, boolean template) {
    exp = exp.replaceAll("#conf\\((.*)\\)", "#conf(#records,$1)");
    exp = exp.replaceAll("#\\{([^.:]*)\\}", "#{#sanitize($1)}");
    
    ExpressionParser parser = new SpelExpressionParser();
    
    if (template) {
      return parser.parseExpression(exp, new TemplateParserContext());  
    } else {
      return parser.parseExpression(exp);
    }
  }

  private String getMetricsPath() throws IOException {
      String metricsPath = null;
      
      String defaultRule = "c:#{#cluster}.q:#{#queue}.u:#{#user}.all.s:#{#status}.j:#{#jobName}";
      
      StandardEvaluationContext context = new StandardEvaluationContext();
      Map<String, Object> systemTokens = getSystemTokens();
      Map<String, String> userTokens = getUserTags();
      
      try {
        context.registerFunction("path", GraphiteHistoryWriter.class.getDeclaredMethod("getDotPath", String[].class));
        context.registerFunction("sanitize", GraphiteHistoryWriter.class.getDeclaredMethod("sanitize",String.class));
        context.registerFunction("conf", GraphiteHistoryWriter.class.getDeclaredMethod("getJobConfProp", new Class[] {JobHistoryRecordCollection.class, String.class}));
      } catch (SecurityException e) {
        LOG.error("SecurityException while adding methods to SEPL context", e);
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        LOG.error("NoSuchMethodException while adding methods to SEPL context", e);
        throw new RuntimeException(e);
      }
      context.setVariables(systemTokens);
      context.setVariable("tag", userTokens);
      context.setVariable("records", recordCollection);
      
      boolean ruleMatched = false;
      
      int numRule = 0;
      for (NamingRule rule: sinkConfig.getMetricNamingRules()) {
        Expression filterExp = getParsedExpression(rule.getFilter(), false);
        if (filterExp.getValue(context, Boolean.class)) {
          incrementCounter("RULE_" + numRule + "_MATCHED", 1);
          String regJobName = recordCollection.getKey().getAppId();
          if (rule.getReplace() != null) {
            int numReplaceRule = 0;
            for (ReplaceRule replaceRule : rule.getReplace()) {
              Pattern replacePattern = Pattern.compile(replaceRule.getRegex());
              if (replacePattern.matcher(recordCollection.getKey().getAppId()).matches()) {
                  incrementCounter("RULE_" + numRule + "_REPLACE_" + numReplaceRule + "_MATCHED", 1);
                  regJobName = replacePattern.matcher(recordCollection.getKey().getAppId()).replaceAll(replaceRule.getWith());
                  break;
              }
              numReplaceRule++;
            }
          }
          context.setVariable("regJobName", regJobName);
          
          Expression nameExp = getParsedExpression(rule.getName(), true);
          metricsPath = nameExp.getValue(context, String.class);
          ruleMatched = true;
          break;
        }
        numRule++;
      }
      
      if (!ruleMatched) {
        incrementCounter("NO_RULE_MATCHED", 1);
//        Expression exp = getParsedExpression(defaultRule, true);
//        metricsPath = exp.getValue(context, String.class);
        LOG.warn("Defaulting to default metric path naming rule for app " + recordCollection.getKey().toString());
      }
      
      return metricsPath;
  }
  
  private void incrementCounter(Counters counter) {
    incrementCounter(sinkConfig.getName() + "_" + counter, 1);
  }
  
  private void incrementCounter(Counters counter, int count) {
    HadoopCompat.incrementCounter(
      taskContext.getCounter(sinkConfig.getName() + "_" + Counters.GRAPHITE_SINK.toString(), counter.toString()), count);
  }
  
  private void incrementCounter(String counter, int count) {
    HadoopCompat.incrementCounter(
      taskContext.getCounter(sinkConfig.getName() + "_" + Counters.GRAPHITE_SINK.toString(), counter), count);
  }
  
  public void write() throws IOException {
    /*
     * Send metrics in the format {PREFIX}.{metricsPath} {value} {submit_time}
     * {metricsPath} is formed using a rule based tokenized format
     */

    int lineCount = 0;
    
    StringBuilder lines = new StringBuilder();
    
    if (filterApp()) {
      incrementCounter(Counters.APPS_FILTERED_IN);
      
      String metricsPath = sinkConfig.getGraphitePrefix() + "." + getMetricsPath();
      
      LOG.info("Sending metrics for: "+ metricsPath);
      try {
        storeAppIdMapping(metricsPath);
      } catch (IOException e) {
        LOG.error("Failed to store mapping for app " + recordCollection.getKey().getAppId()
            + " to '" + metricsPath + "'");
      }

      // Round the timestamp to second as Graphite accepts it in such
      // a format.
      int timestamp = Math.round(recordCollection.getSubmitTime() / 1000);
      
      // For now, relies on receiving job history and job conf as part of the same
      // JobHistoryMultiRecord
      for (JobHistoryRecord jobRecord : recordCollection) {
        if (service == HravenService.JOB_HISTORY
            && (jobRecord.getDataCategory() == RecordCategory.HISTORY_COUNTER || jobRecord
                .getDataCategory() == RecordCategory.INFERRED)
            && !(jobRecord.getDataKey().get(0).equalsIgnoreCase(submitTimeKey)
                || jobRecord.getDataKey().get(0).equalsIgnoreCase(launchTimeKey) || jobRecord.getDataKey()
                .get(0).equalsIgnoreCase(finishTimeKey))) {

          StringBuilder line = new StringBuilder();
          line.append(metricsPath);

          boolean ignoreRecord = false;
          for (String comp : jobRecord.getDataKey().getComponents()) {
            if (excludedComponents != null && excludedComponents.contains(comp)) {
              ignoreRecord = true;
              LOG.info("Excluding component '" + jobRecord.getDataKey().toString() + "' of app " + jobRecord.getKey().toString());
              incrementCounter(Counters.APP_EXCLUDED_COMPS);
              break;
            }
            line.append(".").append(sanitize(comp));
          }
          
          if (ignoreRecord)
            continue;
          else
            lineCount++;

          line.append(" ").append(jobRecord.getDataValue()).append(" ")
              .append(timestamp).append("\n");
          lines .append(line);
        }
      }
      
      //handle run times
      Long finishTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(finishTimeKey));
      if (finishTime == null)
        finishTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(finishTimeKey.toLowerCase()));
      Long launchTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(launchTimeKey));
      if (launchTime == null)
        launchTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(launchTimeKey.toLowerCase()));
      
      if (finishTime != null && recordCollection.getSubmitTime() != null) {
        lines.append(metricsPath + ".").append(totalTimeKey + " " + (finishTime-recordCollection.getSubmitTime()) + " " + timestamp + "\n");
        lineCount++;
      }
      
      if (finishTime != null && launchTime != null) {
        lines.append(metricsPath + ".").append(runTimeKey + " " + (finishTime-launchTime) + " " + timestamp + "\n");
        lineCount++;
      }
    } else {
      incrementCounter(Counters.APPS_FILTERED_OUT);
    }
    
    incrementCounter(Counters.METRICS_WRITTEN, lineCount);
    
    LOG.info("SendToGraphite: " + recordCollection.getKey().toString() + " : " + lines + " metrics"  + "(config: " + sinkConfig.getName() + ")");
    socketWriter.write(lines.toString());
  }

  private void storeAppIdMapping(String metricsPathPrefix) throws IOException {
    Put put = new Put(new JobKeyConverter().toBytes(recordCollection.getKey()));
    put.add(Constants.INFO_FAM_BYTES, Constants.GRAPHITE_KEY_MAPPING_COLUMN_BYTES, Bytes.toBytes(metricsPathPrefix));
    keyMappingTable.put(put);
    
    put = new Put(Bytes.toBytes(metricsPathPrefix));
    
    byte[] appIdBytes = ByteUtil.join(Constants.SEP_BYTES,
      Bytes.toBytes(recordCollection.getKey().getCluster()),
      Bytes.toBytes(recordCollection.getKey().getUserName()),
      Bytes.toBytes(recordCollection.getKey().getAppId()));
      
    put.add(Constants.INFO_FAM_BYTES, Constants.GRAPHITE_KEY_MAPPING_COLUMN_BYTES, appIdBytes);
    reverseKeyMappingTable.put(put);
  }

  private String getPigAlias() {
    return (String) recordCollection.getValue(RecordCategory.CONF, new RecordDataKey("pig.alias"));
  }
  
  private String getPigFeature() {
    return (String) recordCollection.getValue(RecordCategory.CONF, new RecordDataKey("pig.job.feature"));
  }
  
  private String getPigAliasFingerprint() {
    String alias = getPigAlias();
    String feature = getPigFeature();

    if (alias != null) {
      return (feature != null ? feature + ":" : "")
          + StringUtils.abbreviate(alias, PIG_ALIAS_FINGERPRINT_LENGTH);
    }

    return null;
  }

  private Framework getFramework() {
    Object rec =
        recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(Constants.FRAMEWORK_COLUMN));

    if (rec != null) {
      return Framework.valueOf((String) rec);
    }

    return null;
  }
  
  private String getVersion() {
    return (String) recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(Constants.VERSION_COLUMN));
  }
  
  private String getQueue() {
    return (String)recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(Constants.HRAVEN_QUEUE));
  }
  
  private String getJobStatus() {
    return (String)recordCollection.getValue(RecordCategory.HISTORY_META, new RecordDataKey(JobHistoryKeys.JOB_STATUS.toString()));
  }
  
  private String getJobPriority() {
    return (String)recordCollection.getValue(RecordCategory.HISTORY_META, new RecordDataKey(JobHistoryKeys.JOB_PRIORITY.toString()));
  }

  private String getOozieActionName() {
    Object rec = recordCollection.getValue(RecordCategory.CONF, new RecordDataKey("oozie.action.id"));

    if (rec != null) {
      String actionId = ((String) rec);
      return actionId.substring(actionId.indexOf("@") + 1, actionId.length());
    }

    return null;
  }
  
  private String getOozieLauncherPattern() {
    String appId = recordCollection.getKey().getAppId();
    if (APPID_PATTERN_OOZIE_LAUNCHER.matcher(appId).matches()) {
      return APPID_PATTERN_OOZIE_LAUNCHER.matcher(appId).replaceAll("ozl:$1:$2:$3");
    }
    return null;
  }
  
  private String getOozieActionPattern() {
    String appId = recordCollection.getKey().getAppId();
    if (APPID_PATTERN_OOZIE_ACTION.matcher(appId).matches()) {
      return APPID_PATTERN_OOZIE_ACTION.matcher(appId).replaceAll("oza:$1:$2:$3:$4");
    }
    return null;
  }

  /**
   * Util method to generate metrix path prefix
   * @return
   * @throws UnsupportedEncodingException
   */

  public static StringBuilder getDotPath(String... args) {
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
  public static String sanitize(String s) {
    s = StringUtils.isEmpty(s) ? "#null" : s;
    return s.replaceAll(GRAPHITE_KEY_FILTER, "_");
  }

  public static String getJobConfProp(JobHistoryRecordCollection recordCollection, String key) {
    return (String)recordCollection.getValue(RecordCategory.CONF, new RecordDataKey(key));
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
