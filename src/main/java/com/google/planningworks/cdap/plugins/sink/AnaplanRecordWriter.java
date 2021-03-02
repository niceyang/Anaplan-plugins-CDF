package com.google.planningworks.cdap.plugins.sink;

import com.anaplan.client.AnaplanService;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnaplanRecordWriter extends RecordWriter<NullWritable, String> {

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanRecordWriter.class);
  private OutputStream uploadOutputStream;
  //   private final List<Mutation> mutations;
  //   private final int batchSize;


  public AnaplanRecordWriter(Configuration config) {
    AnaplanService.setAPIRoot(config.get(AnaplanSinkConfig.SERVICE_LOCATION), config.get(
        AnaplanSinkConfig.AUTH_SERVICE_LOCATION));
    AnaplanService.setCredential(config.get(AnaplanSinkConfig.USER_NAME), config.get(
        AnaplanSinkConfig.PASSWORD));
    this.uploadOutputStream = AnaplanService.getUploadServerFileOutputStream(
        config.get(AnaplanSinkConfig.WORKSPACE_ID),
        config.get(AnaplanSinkConfig.MODEL_ID),
        config.get(AnaplanSinkConfig.FILE_NAME),
        AnaplanSinkConfig.CHUNK_SIZE);

    // this.mutations = new ArrayList<>();
    // this.batchSize = batchSize;
  }

  @Override
  public void write(NullWritable nullWritable, String input) throws IOException {
    uploadOutputStream.write(input.getBytes());
    // mutations.add(mutation);
    // if (mutations.size() > batchSize) {
    //   databaseClient.write(mutations);
    //   mutations.clear();
    // }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    if (uploadOutputStream != null) {
      uploadOutputStream.close();
      uploadOutputStream = null;
      AnaplanService.closeDown();
    }
    // LOG.error("Writer -- close");
    // try {
    //   if (mutations.size() > 0) {
    //     databaseClient.write(mutations);
    //     taskAttemptContext.getCounter(FileOutputFormatCounter.BYTES_WRITTEN).increment(counter.getValue());
    //     mutations.clear();
    //   }
    // } finally {
    //   spanner.close();
    // }
  }
}

