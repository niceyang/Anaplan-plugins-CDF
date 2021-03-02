package com.google.planningworks.cdap.plugins.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(AnaplanSinkPlugin.PLUGIN_NAME) // <- NOTE: The name of the action should match the name of the docs and widget json files.
@Description("Here is the description for sink.")
public class AnaplanSinkPlugin extends BatchSink<StructuredRecord, NullWritable, String>{

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanSinkPlugin.class);
  private final AnaplanSinkConfig config;

  public static final String PLUGIN_NAME = "AnaplanSink";

  boolean firstLine = true;

  public AnaplanSinkPlugin(AnaplanSinkConfig config) {
    this.config = config;
  }

  /**
   * This function is executed by the Pipelines framework when the Pipeline is deployed. This
   * is a good place to validate any configuration options the user has entered. If this throws
   * an exception, the Pipeline will not be deployed and the user will be shown the error message.
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    // config.validate();
  }

  // prepareRun is called before every pipeline run, and is used to configure what the input should be,
  // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    FailureCollector collector = batchSinkContext.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    LOG.info("PrepareRun -------");

    Configuration configuration = new Configuration();
    AnaplanSinkOutputFormat.configure(configuration, config);

    batchSinkContext.addOutput(Output.of("AnaplanSinkOutPut",
        new SinkOutputFormatProvider(AnaplanSinkOutputFormat.class, configuration)));
  }

  // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    // perform any actions that should happen at the end of the run.
    LOG.info("onRunFinish -------");
  }

  // initialize is called by each job executor before any call to transform is made.
  // This occurs at the start of the batch job run, after the job has been successfully submitted.
  // For example, if mapreduce is the execution engine, each mapper will call initialize at the start of the program.
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    // create any resources required by transform()
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, String>> emitter) {
    if (firstLine) {
      emitter.emit(new KeyValue<>(null, getData(input, /* getHeader */true)));
      firstLine = false;
    }
    emitter.emit(new KeyValue<>(null, getData(input, /* getHeader */false)));
  }

  @Override
  public void destroy() {
    LOG.error("Destroy run -------");
  }


  private static String getData(StructuredRecord input, boolean getHeader) {
    List<Field> fields = input.getSchema().getFields();
    StringBuilder sb = new StringBuilder();
    for (Field field : fields) {
      Object val = null;
      if (getHeader) {
        val = field.getName();
      } else {
        val = input.get(field.getName());
        val = val == null ? "" : val;
      }
      sb.append(val).append(",");
    }
    sb.setLength(sb.length() - 1);
    sb.append("\n");
    return sb.toString();
  }
}

