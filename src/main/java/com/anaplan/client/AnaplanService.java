package com.anaplan.client;

import static com.anaplan.client.Program.fetchChunkSize;
import static com.anaplan.client.Program.getAction;
import static com.anaplan.client.Program.getModel;
import static com.anaplan.client.Program.getProcess;
import static com.anaplan.client.Program.getServerFile;
import static com.anaplan.client.Program.setAuthServiceLocation;
import static com.anaplan.client.Program.setPassphrase;
import static com.anaplan.client.Program.setServiceLocation;
import static com.anaplan.client.Program.setUsername;

import com.anaplan.client.ex.AnaplanAPIException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnaplanService {

  public static enum FunctionType {
    PROCESS,
    ACTION
  }

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanService.class);

  public static void testApi() throws URISyntaxException, InterruptedException {
    String user = "finworks_anaplan_role_account_dev@google.com";
    String pass = "/!bKJ.a/MDQv16CI%uT+";
    String rootURL = "https://google.anaplan.com";
    String workspaceId = "8a81b01168d4b79b0169da7d8575195e";
    String modelId = "F8E1838232FA439D928F05764E763232";
    String processId = "Import - Internal Orders Hierarchy";

    setCredential(user, pass);
    setAPIRoot(rootURL, rootURL);
    // runAnaplanFunction(workspaceId, modelId, processId, FunctionType.PROCESS);

    closeDown();

  }

  public static void main(String[] args)
      throws InterruptedException, IOException, URISyntaxException {
    AnaplanService service = new AnaplanService();
    // service.testExport();
    service.test();
    // service.testApi();

    // System.exit(1);
  }

  public static void setCredential(String user, String pass) {
    setUsername(user);
    setPassphrase(pass);
  }

  public static void setAPIRoot(String serviceLocation, String authServiceLocation) {
    try {
      setServiceLocation(new URI(serviceLocation));
      setAuthServiceLocation(new URI(authServiceLocation));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static void closeDown() {
    Program.closeDown();
  }

  public static void runAnaplanFunction(String workspaceId, String modelId, String functionId, FunctionType type)
      throws InterruptedException {
    TaskFactory taskFactory = null;
    switch (type) {
      case PROCESS:
        taskFactory = getProcess(workspaceId, modelId, functionId);
        break;
      case ACTION:
        taskFactory = getAction(workspaceId, modelId, functionId);
        break;
    }

    if (taskFactory == null) {
      LOG.error("An import, export, action or process must be specified before");
      return; // TODO Throw exception here..
    }

    Task task = taskFactory.createTask(new TaskParameters());
    TaskResult lastResult = task.runTask();
    LOG.info("Function run is done successfully:" + lastResult.isSuccessful());
  }

  public static OutputStream getUploadServerFileOutputStream(String workspaceId, String modelId, String fileId, int chunkSize) {
    ServerFile serverFile = getServerFile(workspaceId, modelId, fileId, true);
    if (serverFile == null) {
      throw new AnaplanAPIException(String.format("Server file: %s is not found from workspace: %s and model: %s", fileId, workspaceId, modelId));
    }
    return serverFile.getUploadStream(fetchChunkSize(String.valueOf(chunkSize))); // in MB
  }

  public static InputStream getDownloadServerFileInputStream(String workspaceId, String modelId, String fileId) {
    ServerFile serverFile = getServerFile(workspaceId, modelId, fileId, false);
    if (serverFile == null) {
      throw new AnaplanAPIException(String.format("Server file: %s is not found from workspace: %s and model: %s", fileId, workspaceId, modelId));
    }
    return serverFile.getDownloadStream();
  }



  public static void  testExport() throws URISyntaxException, InterruptedException, IOException {
    String user = "finworks_anaplan_role_account_dev@google.com";
    String pass = "/!bKJ.a/MDQv16CI%uT+";
    String rootURL = "https://google.anaplan.com";

    setCredential(user, pass);
    setAPIRoot(rootURL, rootURL);

    String workspaceId = "8a81b01068d4b6820169da807d121d00";
    // workspaceId = "8a81b01168d4b79b0169da7d8575195e";
    // modelId = "ACEFE876464E4A30B80E33AB42428A45";
    String modelId = "6387EC16A2104DECA9F56AB3C9BD542D";// export
    // modelId = "F8E1838232FA439D928F05764E763232";
    // service = getService();

    Model model = getModel(workspaceId, modelId);

    LOG.info("-- ServerFiles");

    for (ServerFile serverFile : model.getServerFiles()) {
      LOG.info(Utils.formatTSV(
          serverFile.getId(),
          serverFile.getCode(),
          serverFile.getName()));
    }

    LOG.info("-- Exports");

    for (Export serverExport : model.getExports()) {
      LOG.info(Utils.formatTSV(
          serverExport.getId(),
          serverExport.getCode(),
          serverExport.getName()));
    }

    LOG.info("-- getProcesses");

    for (Process serverProcess : model.getProcesses()) {
      LOG.info(Utils.formatTSV(
          serverProcess.getId(),
          serverProcess.getCode(),
          serverProcess.getName()));
    }

    String sourceId = "Export: BPC - Annual Plan.csv";
    ServerFile serverFile = getServerFile(workspaceId, modelId, sourceId, false);
    InputStream inputStream = serverFile.getDownloadStream();
    byte[] buffer = new byte[4096];
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

    int read;
    do {
      if (0 < (read = inputStream.read(buffer))) {
        String s = new String(buffer, StandardCharsets.UTF_8);
        outputStream.write(s.getBytes());
      }
    } while (-1 != read);
    System.out.println(outputStream.toString());
    inputStream.close();
    LOG.info("-- done");
    closeDown();
  }

  public void test() throws URISyntaxException, InterruptedException, IOException {
    String user = "finworks_anaplan_role_account_dev@google.com";
    String pass = "/!bKJ.a/MDQv16CI%uT+";
    String rootURL = "https://google.anaplan.com";
    setUsername(user);
    setPassphrase(pass);
    setServiceLocation(new URI(rootURL));
    setAuthServiceLocation(new URI(rootURL));
    setUsername(user);
    setPassphrase(pass);
    // workspaceId = "8a81b01068d4b6820169da807d121d00";
    String workspaceId = "8a81b01168d4b79b0169da7d8575195e";
    // modelId = "ACEFE876464E4A30B80E33AB42428A45";
    String modelId = "F8E1838232FA439D928F05764E763232";

    Model model = getModel(workspaceId, modelId);

    for (ServerFile serverFile : model.getServerFiles()) {
      LOG.info(Utils.formatTSV(
          serverFile.getId(),
          serverFile.getCode(),
          serverFile.getName()));
    }

    // for (Export serverExport : model.getExports()) {
    //     LOG.info(Utils.formatTSV(
    //         serverExport.getId(),
    //         serverExport.getCode(),
    //         serverExport.getName()));
    // }

    /* Upload a file */
    String fileId = "anaplan_sap_internal_orders.csv";
    ServerFile serverFile2 = getServerFile(workspaceId, modelId, fileId, true);

    if (serverFile2 == null) {
      LOG.info("no this file");
      return;
    }

    OutputStream uploadStream2 = getUploadServerFileOutputStream(workspaceId, modelId, fileId, 1);
        // serverFile2.getUploadStream(fetchChunkSize("1")); // 1 MB
    byte[] ddata = ("code,description,company_code,profit_center_code\n"
        + "R23,,,\n"
        + "000000012019,,,\n"
        + "000000100001,Sanford test Internal Order,1000,\n"
        + "P1001D0003,,,\n"
        + "P1321D5900,,,\n"
        + "P1001D0005,,,\n"
        + "P1001D1009,,,\n"
        + "000000022019,,,\n"
        + "000000022020,,,\n"
        + "P1001D0006,,,\n"
        + "000000012020,,,\n"
        + "P1001D0002,,,\n"
        + "P1001D1006,,,\n"
        + "000000300000,,1003,\n"
        + "100113A,,,\n"
        + "P100100100,IO Testing new changes,1001,\n"
        + "P8075A0100,Adwords,8075,A01\n"
        + "P8048A0100,Adwords,8048,A01").getBytes();
    byte[] d1 = "code,description,company_code,profit_center_code\n".getBytes();
    byte[] d2 = "000000100001,Sanford test Internal Order,1000,\n".getBytes();
    byte[] d3 = "P8075A0100,Adwords,8075,A01\n".getBytes();
    byte[] d4 = "P100100100,IO Testing new changes,1001,\n".getBytes();
    // byte[] buf2 = new byte[4096];
    // int read2;
    // do {
    //     if (0 < (read2 = System.in.read(buf2))) {
    //
    //     }
    // } while (-1 != read2);
    synchronized(this){
      uploadStream2.write(d1);
      uploadStream2.write(d2);
      uploadStream2.write(d3);
      uploadStream2.write(d4);

      // uploadStream2.flush();
      // uploadStream2.flush();
      // uploadStream2.close();
      uploadStream2.close();
      // uploadStream2.flush();
    }

    LOG.info("Upload to " + fileId + " completed.");


    /* run process */
    String processId = "Import - Internal Orders Hierarchy";
    TaskFactory taskFactory2 = null;
    taskFactory2 = getProcess(workspaceId, modelId, processId);

    if (taskFactory2 != null) {
      TaskParameters taskParameters = new TaskParameters();
      Task task = taskFactory2.createTask(taskParameters);
      TaskResult lastResult = task.runTask();
      System.out.println(lastResult.isSuccessful());
    } else {
      LOG.error("An import, export, action or "
          + "process must be specified before ");
    }
    closeDown();
  }
}

