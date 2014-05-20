package com.twitter.hraven;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

public class RecordDataKey {
  private List<String> components;

  public RecordDataKey(String[] components) {
    this.components = Arrays.asList(components);
  }

  public RecordDataKey(String firstComponent) {
    this.components = new ArrayList<String>();
    this.components.add(firstComponent);
  }

  public void add(String component) {
    this.components.add(component);
  }

  public String get(int index) {
    return components.get(index);
  }

  public List<String> getComponents() {
    return components;
  }

  @Override
  public String toString() {
    return StringUtils.join(Constants.SEP_CHAR, components);
  }
}
