package com.twitter.hraven.etl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.QualifiedJobIdConverter;

/*
 * Marks jobs already loaded into job_history_raw to be reprocessed
 * Subsequently one may run JobFileProcessor with the -r option to process them.
 */
public class JobFileReprocessor extends Configured implements Tool {

    private static final String DATE_FORMAT = "dd/MM/yyyy-HH:mm:ss";
    final static String NAME = JobFileReprocessor.class.getSimpleName();
    private static Log LOG = LogFactory.getLog(JobFileReprocessor.class);

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();

        Option o = new Option("c", "cluster", true,
            "cluster for which jobs are to be  reprocessed");
        o.setArgName("cluster");
        o.setRequired(true);
        options.addOption(o);
        
        o = new Option("sd", "startDate", true,
                "startDate in the format " + DATE_FORMAT);
            o.setArgName("startDate");
            o.setRequired(true);
            options.addOption(o);
        
        o = new Option("ed", "endDate", true,
                "endDate in the format " + DATE_FORMAT);
            o.setArgName("endDate");
            o.setRequired(true);
            options.addOption(o);
        
        o = new Option("a", "apps", true,
                "comma separated list of apps to filter by - has to be part of job history file name");
            o.setArgName("appFilter");
            options.addOption(o);

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
        try {
          commandLine = parser.parse(options, args);
        } catch (Exception e) {
          System.err.println("ERROR: " + e.getMessage() + "\n");
          HelpFormatter formatter = new HelpFormatter();
          formatter.printHelp(NAME + " ", options, true);
          System.exit(-1);
        }

        // Set debug level right away
        if (commandLine.hasOption("d")) {
          Logger log = Logger.getLogger(JobFileProcessor.class);
          log.setLevel(Level.DEBUG);
        }

        return commandLine;
      }

    public void reprocessJobs(String cluster, String startDate, String endDate, List<String> includeApps, Configuration hbaseConf) throws IOException, ParseException {
        QualifiedJobIdConverter idConv = new QualifiedJobIdConverter();
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

        HTable jobHistoryRaw = new HTable(hbaseConf, Constants.HISTORY_RAW_TABLE_BYTES);
        Scan scan = new Scan();
        scan.addFamily(Constants.INFO_FAM_BYTES);
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        byte[] clusterPrefix = Bytes.toBytes(cluster + Constants.SEP);
        PrefixFilter prefixFilter = new PrefixFilter(clusterPrefix);
        filters.addFilter(prefixFilter);
        SingleColumnValueFilter startDateFilter =
                new SingleColumnValueFilter(Constants.INFO_FAM_BYTES,
                        Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES,
                        CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(sdf
                                .parse(startDate).getTime()));
        SingleColumnValueFilter endDateFilter =
                new SingleColumnValueFilter(Constants.INFO_FAM_BYTES,
                        Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES,
                        CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(sdf.parse(endDate)
                                .getTime()));
        startDateFilter.setFilterIfMissing(true);
        endDateFilter.setFilterIfMissing(true);
        filters.addFilter(startDateFilter);
        filters.addFilter(endDateFilter);

        if (includeApps != null) {
            FilterList includeAppFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);

            for (String includeApp: includeApps) {
                includeAppFilter.addFilter(new SingleColumnValueFilter(Constants.INFO_FAM_BYTES,
                        Constants.JOBHISTORY_FILENAME_COL_BYTES, CompareOp.EQUAL,
                        new RegexStringComparator(".*" + includeApp + ".*")));
            }

            filters.addFilter(includeAppFilter);
        }

        scan.setFilter(filters);
        scan.setCaching(1000);

        for (Result result : jobHistoryRaw.getScanner(scan)) {
            byte[] jobhistory_last_modified_value =
                    result.getValue(Bytes.toBytes("i"),
                            Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES);
            Date jobhistory_last_modified = null;
            if (jobhistory_last_modified_value != null) {
                jobhistory_last_modified = new Date(Bytes.toLong(jobhistory_last_modified_value));
            }
            byte[] jobhistory_filename =
                    result.getValue(Bytes.toBytes("i"), Constants.JOBHISTORY_FILENAME_COL_BYTES);

            LOG.info("Marking job to be reprocessed: " + idConv.fromBytes(result.getRow())
                    + ", time: " + jobhistory_last_modified + ", filename: "
                    + Bytes.toString(jobhistory_filename));
            Put put = new Put(result.getRow());
            put.add(Constants.INFO_FAM_BYTES, Constants.RAW_COL_REPROCESS_BYTES,
                    Bytes.toBytes(true));
            jobHistoryRaw.put(put);
        }

        jobHistoryRaw.close();
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration hbaseConf = HBaseConfiguration.create(getConf());
        
        String[] otherArgs = new GenericOptionsParser(hbaseConf, args).getRemainingArgs();
        CommandLine commandLine = parseArgs(otherArgs);
        
        String cluster = commandLine.getOptionValue("c");
        String startDate = commandLine.getOptionValue("sd");
        String endDate = commandLine.getOptionValue("ed");
        String includeAppStr = commandLine.getOptionValue("a");

        if (includeAppStr != null) {
            List<String> includeApps = Arrays.asList(includeAppStr.split(","));
            if (includeApps.size() > 5) {
                LOG.info("More than 5 apps to filter. Splitting into batches");
                int appI = 0;
                while (appI < includeApps.size()) {
                    LOG.info("Reprocessing app filter batch " + appI + " to " + (int)((int)appI+(int)5));
                    reprocessJobs(cluster, startDate, endDate, includeApps.subList(appI,
                            appI + 5 <= includeApps.size() ? appI + 5 : includeApps.size()),
                            hbaseConf);
                    appI += 5;
                }
            } else {
                LOG.info("Reprocessing with app filter: " + includeAppStr);
                reprocessJobs(cluster, startDate, endDate, includeApps, hbaseConf);
            }
        } else {
            reprocessJobs(cluster, startDate, endDate, null, hbaseConf);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new JobFileReprocessor(), args);

        if (res == 1)
            throw new RuntimeException("Job Failed");
    }
}
