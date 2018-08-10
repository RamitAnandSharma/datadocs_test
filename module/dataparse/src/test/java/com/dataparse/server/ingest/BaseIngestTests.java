package com.dataparse.server.ingest;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.controllers.api.table.SearchIndexRequest;
import com.dataparse.server.controllers.api.table.SearchIndexResponse;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.util.FileUploadUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

@Slf4j
public class BaseIngestTests extends IsolatedContextTest {
  final static public int MAX_PROCESSING_TIME = 10000;
  @Autowired
  private TableService tableService;

  @Autowired
  private FileUploadUtils fileUploadUtils;

  @Autowired
  private TaskManagementService taskManagementService;

  @Autowired
  private VisualizationService visualizationService;

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private TableRepository tableRepository;
  
  public static Collection<IngestInput> data = Lists.newArrayList(
      new IngestInput("test.xml", 2, 4, MAX_PROCESSING_TIME, DataFormat.CONTENT_TYPE_TEXT_XML)
//      new IngestInput("test.csv", 6, 3, MAX_PROCESSING_TIME, DataFormat.CONTENT_TYPE_CSV)
//      new IngestInput("test_array.json",2, 5, MAX_PROCESSING_TIME, DataFormat.CONTENT_TYPE_JSON),
//      new IngestInput("test_one_tab.xlsx",6, 4, MAX_PROCESSING_TIME, DataFormat.CONTENT_TYPE_XLSX)
   );

  @Test
  public void ingestTest() throws Exception {
    for (IngestInput ingestInput : data) {
      log.info("Start processing {}", ingestInput.getName());
      User u = new User("user" + ingestInput.getName(), "user1");
      u.setRegistered(true);
      User user = userRepository.saveUser(u);
      Auth.set(new Auth(user.getId(), ""));
      long start = System.currentTimeMillis();
      Upload file = fileUploadUtils.createFile(ingestInput.getName(), user.getId(), null, ingestInput.getContentType());
      CreateDatadocRequest createDoc = new CreateDatadocRequest("d", null, "id:" + file.getId(), true, true, start, true);
      Datadoc datadoc = tableService.createDatadoc(createDoc);
      List<String> tasks = datadoc.getLastFlowExecutionTasks();
      long processionTime = System.currentTimeMillis() - start;
      log.info("File {} ingested for {}", ingestInput.getName(), processionTime);
      if(processionTime > ingestInput.getMaxTime()) {
        throw new RuntimeException("Creating datadoc took too much time. " + ingestInput);
      }

      taskManagementService.waitUntilFinished(tasks);
      List<TableBookmark> tableBookmarks = tableRepository.getTableBookmarks(datadoc.getId(), true);
      Assert.assertEquals(tableBookmarks.size(), 1);
      TableBookmark tableBookmark = tableBookmarks.get(0);

      SearchIndexRequest request = SearchIndexRequest.builder()
          .tableId(datadoc.getId())
          .tableBookmarkId(tableBookmark.getId())
          .stateId(tableBookmark.getBookmarkStateId().getStateId())
          .from(0L)
          .build();

      SearchIndexResponse response = visualizationService.search(request);
      int rowsCount = response.getData().getChildren().size();
      Assert.assertTrue(ingestInput.getExpectedRowsCount() == rowsCount);

      Set<String> columns = new HashSet<>(response.getData().getChildren().get(0).getData().keySet());
      columns.remove("es_metadata_id");
      Assert.assertTrue(columns.size() == ingestInput.getExpectedColumnsCount());
    }
  }
}
