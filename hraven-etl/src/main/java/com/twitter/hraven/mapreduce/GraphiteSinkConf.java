package com.twitter.hraven.mapreduce;

import static com.twitter.hraven.Constants.*;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class GraphiteSinkConf {

    //simple name derived from file name
    private String name;
    
    @JsonProperty(JOBCONF_GRAPHITE_HOST_KEY)
    private String graphiteHost;
    @JsonProperty(JOBCONF_GRAPHITE_PORT_KEY)
    private Integer graphitePort;
    
    // prepend this prefix to all metrics
    @JsonProperty(JOBCONF_GRAPHITE_PREFIX)
    private String graphitePrefix;
    
    // filter jobs not submitted by this user
    @JsonProperty(JOBCONF_GRAPHITE_USER_FILTER)
    private String userfilter;
    
    // filter jobs not submitted in this queue
    @JsonProperty(JOBCONF_GRAPHITE_QUEUE_FILTER)
    private String queuefilter;
    
    // exclude these metric path components (e.g MultiInputCounters - create a lot of redundant tree
    // paths, and you wouldn't want to send them to graphite)
    @JsonProperty(JOBCONF_GRAPHITE_EXCLUDED_COMPONENTS)
    private String excludedComponents;
    
    // comma seperated list of app substrings to include
    @JsonProperty(JOBCONF_GRAPHITE_INCLUDE_APPS)
    private String includeApps;
    
    //comma seperated list of app substrings to exclude
    @JsonProperty(JOBCONF_GRAPHITE_EXCLUDE_APPS)
    private String excludeApps;
    
    @JsonProperty(JOBCONF_GRAPHITE_NAMING_RULE_CONFIG)
    private List<NamingRule> metricNamingRules;
   
    @JsonProperty(JOBCONF_GRAPHITE_TIMESTAMP_EXPRESSION)
    private String timestampExpression;
   
    public String getGraphiteHost() {
        return graphiteHost;
    }
    
    public void setGraphiteHost(String graphiteHost) {
        this.graphiteHost = graphiteHost;
    }

    public Integer getGraphitePort() {
        return graphitePort;
    }

    public void setGraphitePort(Integer graphitePort) {
        this.graphitePort = graphitePort;
    }

    public String getGraphitePrefix() {
        return graphitePrefix;
    }

    public void setGraphitePrefix(String graphitePrefix) {
        this.graphitePrefix = graphitePrefix;
    }

    public String getUserfilter() {
        return userfilter;
    }

    public void setUserfilter(String userfilter) {
        this.userfilter = userfilter;
    }

    public String getQueuefilter() {
        return queuefilter;
    }

    public void setQueuefilter(String queuefilter) {
        this.queuefilter = queuefilter;
    }

    public String getExcludedComponents() {
        return excludedComponents;
    }

    public void setExcludedComponents(String excludedComponents) {
        this.excludedComponents = excludedComponents;
    }

    public String getIncludeApps() {
        return includeApps;
    }

    public void setIncludeApps(String includeApps) {
        this.includeApps = includeApps;
    }

    public String getExcludeApps() {
        return excludeApps;
    }

    public void setExcludeApps(String excludeApps) {
        this.excludeApps = excludeApps;
    }

    @Override
    public String toString() {
        return "GraphiteSinkConf [graphiteHost=" + graphiteHost + ", graphitePort=" + graphitePort
                + ", graphitePrefix=" + graphitePrefix + ", userfilter=" + userfilter
                + ", queuefilter=" + queuefilter + ", excludedComponents=" + excludedComponents
                + ", includeApps=" + includeApps + ", excludeApps=" + excludeApps
                + ", metricNamingRules=" + getMetricNamingRules() + "]";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<NamingRule> getMetricNamingRules() {
        return metricNamingRules;
    }

    public void setMetricNamingRules(List<NamingRule> metricNamingRules) {
        this.metricNamingRules = metricNamingRules;
    }

    public String getTimestampExpression() {return timestampExpression;}

    public void setTimestampExpression(String timestampExpression) { this.timestampExpression = timestampExpression;}
}