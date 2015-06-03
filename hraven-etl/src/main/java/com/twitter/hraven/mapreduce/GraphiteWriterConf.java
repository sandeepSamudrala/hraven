package com.twitter.hraven.mapreduce;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonPropertyOrder({
                       "hraven.sink.graphite.host",
                       "hraven.sink.graphite.port",
                       "hraven.sink.graphite.prefix",
                       "hraven.sink.graphite.userfilter",
                       "hraven.sink.graphite.queuefilter",
                       "hraven.sink.graphite.excludedcomponents",
                       "hraven.sink.graphite.includeapps",
                       "hraven.sink.graphite.excludeapps",
                       "hraven.sink.graphite.metric.naming.rules"
                   })
public class GraphiteWriterConf {

    @JsonProperty("hraven.sink.graphite.host")
    private String hravenSinkGraphiteHost;
    @JsonProperty("hraven.sink.graphite.port")
    private Integer hravenSinkGraphitePort;
    @JsonProperty("hraven.sink.graphite.prefix")
    private String hravenSinkGraphitePrefix;
    @JsonProperty("hraven.sink.graphite.userfilter")
    private String hravenSinkGraphiteUserfilter;
    @JsonProperty("hraven.sink.graphite.queuefilter")
    private String hravenSinkGraphiteQueuefilter;
    @JsonProperty("hraven.sink.graphite.excludedcomponents")
    private String hravenSinkGraphiteExcludedcomponents;
    @JsonProperty("hraven.sink.graphite.includeapps")
    private String hravenSinkGraphiteIncludeapps;
    @JsonProperty("hraven.sink.graphite.excludeapps")
    private String hravenSinkGraphiteExcludeapps;
    @JsonProperty("hraven.sink.graphite.metric.naming.rules")
    private String hravenSinkGraphiteMetricNamingRules;
//    @JsonIgnore
//    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     *
     * @return
     * The hravenSinkGraphiteHost
     */
    @JsonProperty("hraven.sink.graphite.host")
    public String getHravenSinkGraphiteHost() {
        return hravenSinkGraphiteHost;
    }

    /**
     *
     * @param hravenSinkGraphiteHost
     * The hraven.sink.graphite.host
     */
    @JsonProperty("hraven.sink.graphite.host")
    public void setHravenSinkGraphiteHost(String hravenSinkGraphiteHost) {
        this.hravenSinkGraphiteHost = hravenSinkGraphiteHost;
    }

    /**
     *
     * @return
     * The hravenSinkGraphitePort
     */
    @JsonProperty("hraven.sink.graphite.port")
    public Integer getHravenSinkGraphitePort() {
        return hravenSinkGraphitePort;
    }

    /**
     *
     * @param hravenSinkGraphitePort
     * The hraven.sink.graphite.port
     */
    @JsonProperty("hraven.sink.graphite.port")
    public void setHravenSinkGraphitePort(Integer hravenSinkGraphitePort) {
        this.hravenSinkGraphitePort = hravenSinkGraphitePort;
    }

    /**
     *
     * @return
     * The hravenSinkGraphitePrefix
     */
    @JsonProperty("hraven.sink.graphite.prefix")
    public String getHravenSinkGraphitePrefix() {
        return hravenSinkGraphitePrefix;
    }

    /**
     *
     * @param hravenSinkGraphitePrefix
     * The hraven.sink.graphite.prefix
     */
    @JsonProperty("hraven.sink.graphite.prefix")
    public void setHravenSinkGraphitePrefix(String hravenSinkGraphitePrefix) {
        this.hravenSinkGraphitePrefix = hravenSinkGraphitePrefix;
    }

    /**
     *
     * @return
     * The hravenSinkGraphiteUserfilter
     */
    @JsonProperty("hraven.sink.graphite.userfilter")
    public String getHravenSinkGraphiteUserfilter() {
        return hravenSinkGraphiteUserfilter;
    }

    /**
     *
     * @param hravenSinkGraphiteUserfilter
     * The hraven.sink.graphite.userfilter
     */
    @JsonProperty("hraven.sink.graphite.userfilter")
    public void setHravenSinkGraphiteUserfilter(String hravenSinkGraphiteUserfilter) {
        this.hravenSinkGraphiteUserfilter = hravenSinkGraphiteUserfilter;
    }

    /**
     *
     * @return
     * The hravenSinkGraphiteQueuefilter
     */
    @JsonProperty("hraven.sink.graphite.queuefilter")
    public String getHravenSinkGraphiteQueuefilter() {
        return hravenSinkGraphiteQueuefilter;
    }

    /**
     *
     * @param hravenSinkGraphiteQueuefilter
     * The hraven.sink.graphite.queuefilter
     */
    @JsonProperty("hraven.sink.graphite.queuefilter")
    public void setHravenSinkGraphiteQueuefilter(String hravenSinkGraphiteQueuefilter) {
        this.hravenSinkGraphiteQueuefilter = hravenSinkGraphiteQueuefilter;
    }

    /**
     *
     * @return
     * The hravenSinkGraphiteExcludedcomponents
     */
    @JsonProperty("hraven.sink.graphite.excludedcomponents")
    public String getHravenSinkGraphiteExcludedcomponents() {
        return hravenSinkGraphiteExcludedcomponents;
    }

    /**
     *
     * @param hravenSinkGraphiteExcludedcomponents
     * The hraven.sink.graphite.excludedcomponents
     */
    @JsonProperty("hraven.sink.graphite.excludedcomponents")
    public void setHravenSinkGraphiteExcludedcomponents(String hravenSinkGraphiteExcludedcomponents) {
        this.hravenSinkGraphiteExcludedcomponents = hravenSinkGraphiteExcludedcomponents;
    }

    /**
     *
     * @return
     * The hravenSinkGraphiteIncludeapps
     */
    @JsonProperty("hraven.sink.graphite.includeapps")
    public String getHravenSinkGraphiteIncludeapps() {
        return hravenSinkGraphiteIncludeapps;
    }

    /**
     *
     * @param hravenSinkGraphiteIncludeapps
     * The hraven.sink.graphite.includeapps
     */
    @JsonProperty("hraven.sink.graphite.includeapps")
    public void setHravenSinkGraphiteIncludeapps(String hravenSinkGraphiteIncludeapps) {
        this.hravenSinkGraphiteIncludeapps = hravenSinkGraphiteIncludeapps;
    }

    /**
     *
     * @return
     * The hravenSinkGraphiteExcludeapps
     */
    @JsonProperty("hraven.sink.graphite.excludeapps")
    public String getHravenSinkGraphiteExcludeapps() {
        return hravenSinkGraphiteExcludeapps;
    }

    /**
     *
     * @param hravenSinkGraphiteExcludeapps
     * The hraven.sink.graphite.excludeapps
     */
    @JsonProperty("hraven.sink.graphite.excludeapps")
    public void setHravenSinkGraphiteExcludeapps(String hravenSinkGraphiteExcludeapps) {
        this.hravenSinkGraphiteExcludeapps = hravenSinkGraphiteExcludeapps;
    }

    /**
     *
     * @return
     * The hravenSinkGraphiteMetricNamingRules
     */
    @JsonProperty("hraven.sink.graphite.metric.naming.rules")
    public String getHravenSinkGraphiteMetricNamingRules() {
        return hravenSinkGraphiteMetricNamingRules;
    }

    /**
     *
     * @param hravenSinkGraphiteMetricNamingRules
     * The hraven.sink.graphite.metric.naming.rules
     */
    @JsonProperty("hraven.sink.graphite.metric.naming.rules")
    public void setHravenSinkGraphiteMetricNamingRules(String hravenSinkGraphiteMetricNamingRules) {
        this.hravenSinkGraphiteMetricNamingRules = hravenSinkGraphiteMetricNamingRules;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

//    @JsonAnyGetter
//    public Map<String, Object> getAdditionalProperties() {
//        return this.additionalProperties;
//    }
//
//    @JsonAnySetter
//    public void setAdditionalProperty(String name, Object value) {
//        this.additionalProperties.put(name, value);
//    }


}