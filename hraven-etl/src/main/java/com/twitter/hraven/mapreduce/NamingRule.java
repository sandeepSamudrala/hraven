package com.twitter.hraven.mapreduce;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class NamingRule {
  @NonNull
  String filter;
  
  @NonNull
  String name;
  
  @NonNull
  List<ReplaceRule> replace;
}
