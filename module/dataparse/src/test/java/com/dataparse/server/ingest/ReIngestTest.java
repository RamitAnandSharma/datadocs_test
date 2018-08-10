package com.dataparse.server.ingest;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.flow.builder.FlowContainerDTO;
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
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static com.dataparse.server.service.flow.FlowGenerator.singleSourceFlow;

@Slf4j
public class ReIngestTest extends IsolatedContextTest {

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

    @Test
    public void simpleReIngestTest() throws Exception {
        IngestInput ingestInput = new IngestInput("test.csv", 6, 3, 10000, DataFormat.CONTENT_TYPE_CSV);
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

        taskManagementService.waitUntilFinished(tasks);
        List<TableBookmark> tableBookmarks = tableRepository.getTableBookmarks(datadoc.getId(), true);
        Assert.assertEquals(tableBookmarks.size(), 1);
        TableBookmark tableBookmark = tableBookmarks.get(0);

        FlowContainerDTO flowContainerDTO = singleSourceFlow(file, tableBookmark.getId());
        flowContainerDTO.getCells();

    }

}
