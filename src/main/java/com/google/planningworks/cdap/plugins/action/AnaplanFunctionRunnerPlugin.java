package com.google.planningworks.cdap.plugins.action;

import com.anaplan.client.AnaplanService;
import com.anaplan.client.AnaplanService.FunctionType;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(type = Action.PLUGIN_TYPE)
@Name(AnaplanFunctionRunnerPlugin.PLUGIN_NAME) // <- NOTE: The name of the action should match the name of the docs and widget json files.
@Description("Here is the description.")
public class AnaplanFunctionRunnerPlugin extends Action{

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanFunctionRunnerPlugin.class);
  private final AnaplanFunctionRunnerConfig config;

  public static final String PLUGIN_NAME = "AnaplanFunctionRunner";

  public AnaplanFunctionRunnerPlugin(AnaplanFunctionRunnerConfig config) {
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
  public void run(ActionContext context) throws Exception {
    LOG.debug(String.format("Running the 'run' method of the %s action.", PLUGIN_NAME));
    AnaplanService.setAPIRoot(config.getServiceLocation(), config.getAuthServiceLocation());
    AnaplanService.setCredential(config.getUserName(), config.getPassword());
    AnaplanService.runAnaplanFunction(
        config.getWorkspaceId(),
        config.getModelId(),
        config.getFunctionName(),
        FunctionType.valueOf(config.getFunctionType()));
    AnaplanService.closeDown();

    // It's a good idea to validate the configuration one last time. This is in case the user
    // entered any macros (E.g. ${file-path}) in the configuration options which can only be
    // validated when the pipeline is executed.
    // config.validate();

    // The main logic of your action action goes here.

    // This is an example of setting arguments that can be used later by plugins in your Pipeline.
    // This can be useful if you are pulling configuration from an external webservice for example.
    // context.getArguments().set("example.action.arg",
    //     "This value can be used by other plugins in the Pipeline by " +
    //         "specifying ${example.action.arg}.");
  }

}

