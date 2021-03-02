package com.google.planningworks.cdap.plugins.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

/**
 * The config class for {@link AnaplanExportPlugin} that contains all properties that need to be filled in by
 * the user when building a Pipeline.
 */
class AnaplanExportConfig extends PluginConfig {

  public static String FILE_CONTENT_TYPE = "text/plain";

  @Name("projectId")
  @Macro
  @Description("GCP Project ID")
  // @Nullable // <-- Indicates that the config is Optional.
  private String projectId;

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
  @Description("Anaplan Server file name")
  private String fileName;

  @Name("gcsBucket")
  @Macro
  @Description("GCS bucket for export file buffer")
  private String gcsBucket;

  @Name("archiveName")
  @Macro
  @Description("File archive name in GCS bucket")
  private String archiveName;

  /**
   * You can leverage this function to validate the configure options entered by the user.
   */
  public void validate() throws IllegalArgumentException {
    containsMacro("D");
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

  public String getFileName() {
    return fileName;
  }

  public String getGcsBucket() {
    return gcsBucket;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getArchiveName() {
    return archiveName;
  }

// @VisibleForTesting
  // public ExampleActionConfig(@Nullable String exampleConfigOption) {
  //   this.exampleConfigOption = exampleConfigOption;
  // }
}