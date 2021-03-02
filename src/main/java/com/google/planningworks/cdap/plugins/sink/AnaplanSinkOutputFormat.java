package com.google.planningworks.cdap.plugins.sink;

import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.AUTH_SERVICE_LOCATION;
import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.FILE_NAME;
import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.MODEL_ID;
import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.PASSWORD;
import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.SERVICE_LOCATION;
import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.USER_NAME;
import static com.google.planningworks.cdap.plugins.sink.AnaplanSinkConfig.WORKSPACE_ID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnaplanSinkOutputFormat extends OutputFormat<NullWritable, String> {

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanSinkOutputFormat.class);

  public static void configure(Configuration configuration, AnaplanSinkConfig config) {
    configuration.set(SERVICE_LOCATION, config.getServiceLocation());
    configuration.set(AUTH_SERVICE_LOCATION, config.getAuthServiceLocation());
    configuration.set(USER_NAME, config.getUserName());
    configuration.set(PASSWORD, config.getPassword());
    configuration.set(WORKSPACE_ID, config.getWorkspaceId());
    configuration.set(MODEL_ID, config.getModelId());
    configuration.set(FILE_NAME, config.getFileName());
  }

  @Override
  public RecordWriter<NullWritable, String> getRecordWriter(TaskAttemptContext context) {
    Configuration config = context.getConfiguration();
    return new AnaplanRecordWriter(config);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
  }

  /**
   * No op output committer
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }



}
