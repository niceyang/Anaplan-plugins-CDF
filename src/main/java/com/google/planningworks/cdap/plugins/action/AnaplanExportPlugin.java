package com.google.planningworks.cdap.plugins.action;

import com.anaplan.client.AnaplanService;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(type = Action.PLUGIN_TYPE)
@Name(AnaplanExportPlugin.PLUGIN_NAME) // <- NOTE: The name of the action should match the name of the docs and widget json files.
@Description("Here is the description.")
public class AnaplanExportPlugin extends Action{

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanExportPlugin.class);
  private final AnaplanExportConfig config;

  public static final String PLUGIN_NAME = "AnaplanExport";

  public AnaplanExportPlugin(AnaplanExportConfig config) {
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
    LOG.debug(String.format("Running the 'configurePipeline' method of the %s action.", PLUGIN_NAME));
    // config.validate();
  }

  @Override
  public void run(ActionContext context) throws IOException {
    LOG.debug(String.format("Running the 'run' method of the %s action.", PLUGIN_NAME));

    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(config.getProjectId());
    Storage storage = builder.build().getService();
    // Create bucket if not exist
    if (storage.get(config.getGcsBucket()) == null) {
      storage.create(BucketInfo.of(config.getGcsBucket()));
    }

    BlobId blobId = BlobId.of(config.getGcsBucket(), config.getArchiveName());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(
        AnaplanExportConfig.FILE_CONTENT_TYPE).build();

    AnaplanService.setAPIRoot(config.getServiceLocation(), config.getAuthServiceLocation());
    AnaplanService.setCredential(config.getUserName(), config.getPassword());
    InputStream inputStream = AnaplanService.getDownloadServerFileInputStream(
        config.getWorkspaceId(),
        config.getModelId(),
        config.getFileName());

    try (WriteChannel writer = storage.writer(blobInfo)) {
      ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
    } catch (IOException ex) {
      throw ex;
    }

    inputStream.close();
    AnaplanService.closeDown();
  }

}

