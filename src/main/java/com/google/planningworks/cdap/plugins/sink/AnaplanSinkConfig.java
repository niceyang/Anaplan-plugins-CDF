package com.google.planningworks.cdap.plugins.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * The config class for {@link AnaplanSinkPlugin} that contains all properties that need to be filled in by
 * the user when building a Pipeline.
 */
class AnaplanSinkConfig extends PluginConfig {

  public static String SERVICE_LOCATION = "SERVICE_LOCATION";
  public static String AUTH_SERVICE_LOCATION = "AUTH_SERVICE_LOCATION";
  public static String USER_NAME = "USER_NAME";
  public static String PASSWORD = "PASSWORD";
  public static String WORKSPACE_ID = "WORKSPACE_ID";
  public static String MODEL_ID = "MODEL_ID";
  public static String FILE_NAME = "FILE_NAME";
  public static int CHUNK_SIZE = 10;

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

  @Name("fileName")
  @Macro
  @Description("Server file name")
  private String fileName;



  /**
   * You can leverage this function to validate the configure options entered by the user.
   * @param collector
   */
  public void validate(FailureCollector collector) throws IllegalArgumentException {
    // try {
    //   new URL(url);
    // } catch (MalformedURLException e) {
    //   collector.addFailure(String.format("URL '%s' is malformed: %s", url, e.getMessage()), null)
    //       .withConfigProperty(URL);
    // }
    // if (!containsMacro(METHOD) && !METHODS.contains(method.toUpperCase())) {
    //   collector.addFailure(
    //       String.format("Invalid request method %s, must be one of %s.", method, Joiner.on(',').join(METHODS)), null)
    //       .withConfigProperty(METHOD);
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

  public String getFileName() {
    return fileName;
  }

// @VisibleForTesting
  // public ExampleActionConfig(@Nullable String exampleConfigOption) {
  //   this.exampleConfigOption = exampleConfigOption;
  // }
}