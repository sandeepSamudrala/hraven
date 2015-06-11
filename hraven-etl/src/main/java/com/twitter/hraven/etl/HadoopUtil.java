package com.twitter.hraven.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.twitter.hraven.Constants;

public class HadoopUtil {
  public static void setTmpJars(String libPathConf, Configuration conf) throws IOException {
    StringBuilder tmpjars = new StringBuilder();
    if (conf.get(libPathConf) != null) {
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] files = fs.listStatus(new Path(conf.get(libPathConf)));
      if (files != null) {
        for (FileStatus file : files) {
          if (!tmpjars.toString().isEmpty()) tmpjars = tmpjars.append(",");
          tmpjars = tmpjars.append(file.getPath());
        }
        conf.set(Constants.HADOOP_TMP_JARS_CONF, tmpjars.toString());
      }
    }
  }

public static String readFsFile(String fsFile, Configuration conf) throws IOException {
    Path path = new Path(fsFile);
    FileSystem fs = null;
    fs = path.getFileSystem(conf);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    StringBuilder everything = new StringBuilder();
    String line;
    while( (line = br.readLine()) != null) {
       everything.append(line);
    }
    return everything.toString();
  }
}
