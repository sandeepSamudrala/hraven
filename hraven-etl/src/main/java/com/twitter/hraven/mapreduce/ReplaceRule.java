package com.twitter.hraven.mapreduce;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class ReplaceRule {
  @NonNull
  String regex;
  
  @NonNull
  String with;
}
