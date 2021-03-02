package com.google.planningworks.cdap.plugins.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

/**
 * The config class for {@link AnaplanFunctionRunnerPlugin} that contains all properties that need to be filled in by
 * the user when building a Pipeline.
 */
class AnaplanFunctionRunnerConfig extends PluginConfig {

  @Name("serviceLocation")
  @Macro
  @Description("Service location of your Anaplan API")
  // @Nullable // <-- Indicates that the config is Optional.
  private String serviceLocation;

  @Name("authServiceLocation")
  @Macro
  @Description("Service location of your Anaplan authentication API")
  private String authServiceLocation;

  @Name("userName")
  @Macro
  @Description("User name")
  private String userName;

  @Name("password")
  @Macro
  @Description("Password")
  private String password;

  @Name("workspaceId")
  @Macro
  @Description("WorkspaceId")
  private String workspaceId;

  @Name("modelId")
  @Macro
  @Description("ModelId")
  private String modelId;

  @Name("functionType")
  @Macro
  @Description("Process or action")
  private String functionType;

  @Name("functionName")
  @Macro
  @Description("Process or action name")
  private String functionName;

  @Override
  public String toString() {
    return "ExampleActionConfig{" +
        "serviceLocation='" + serviceLocation + '\'' +
        ", authServiceLocation='" + authServiceLocation + '\'' +
        ", userName='" + userName + '\'' +
        ", password='" + password + '\'' +
        ", workspaceId='" + workspaceId + '\'' +
        ", modelId='" + modelId + '\'' +
        ", functionName='" + functionName + '\'' +
        '}';
  }

  /**
   * You can leverage this function to validate the configure options entered by the user.
   */
  public void validate() throws IllegalArgumentException {
    // The containsMacro function can be used to check if there is a macro in the config option.
    // At runtime, the containsMacro function will always return false.
    // if (!containsMacro("exampleConfigOption") && !Strings.isNullOrEmpty(exampleConfigOption)) {
    //   if (exampleConfigOption.contains("test")) {
    //     throw new IllegalArgumentException("The config value cannot contain the word 'test' for some reason.");
    //   }
    // }
  }

  public String getServiceLocation() {
    return serviceLocation;
  }

  public String getAuthServiceLocation() {
    return authServiceLocation;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }

  public String getWorkspaceId() {
    return workspaceId;
  }

  public String getModelId() {
    return modelId;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getFunctionType() {
    return functionType;
  }

// @VisibleForTesting
  // public ExampleActionConfig(@Nullable String exampleConfigOption) {
  //   this.exampleConfigOption = exampleConfigOption;
  // }
}