package com.twitter.hraven.mapreduce;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class GraphiteMetricNamingRuleConfig {
  @NonNull
  private List<NamingRule> namingRules; 
}
