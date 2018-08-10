package com.dataparse.server.state;


import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.flow.FlowExecutionRequest;
import com.dataparse.server.service.flow.FlowService;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.upload.UploadService;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.VisualizationService;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.event.BookmarkVizStateChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.aggs.AggAddEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.aggs.AggMoveEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.aggs.AggRemoveEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.shows.ShowAddEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.shows.ShowMoveEvent;
import com.dataparse.server.service.visualization.bookmark_state.event.shows.ShowRemoveEvent;
import com.dataparse.server.service.visualization.bookmark_state.state.Agg;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.Col;
import com.dataparse.server.service.visualization.bookmark_state.state.Show;
import com.dataparse.server.util.FileUploadUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Slf4j
public class StateProviderTest extends IsolatedContextTest {

  @Autowired
  UserRepository userRepository;

  @Autowired
  UploadService uploadService;

  @Autowired
  UploadRepository uploadRepository;

  @Autowired
  TableRepository tableRepository;

  @Autowired
  TableService tableService;

  @Autowired
  TaskManagementService taskManagementService;

  @Autowired
  FlowService flowService;

  @Autowired
  BookmarkStateStorage bookmarkStateStorage;

  @Autowired
  VisualizationService visualizationService;

  @Autowired
  FileUploadUtils uploadUtils;

  private String flowJSONTemplate = "{\"cells\":[{\"type\":\"html.OutputNode\",\"inPorts\":[\"in1\"],\"outPorts\":[],\"position\":{\"x\":620,\"y\":100},\"settings\":{\"@class\":\".OutputNodeSettings\",\"bookmarkId\":%d,\"transforms\":[],\"columns\":[]},\"id\":\"cef50446-0ebb-4902-865e-b56862226497\"},{\"type\":\"html.Link\",\"source\":{\"id\":\"fd638363-8161-449e-886d-745d5a737afc\",\"port\":\"out\"},\"target\":{\"id\":\"cef50446-0ebb-4902-865e-b56862226497\",\"port\":\"in1\"},\"id\":\"4bb6ab22-f0eb-4e29-ae35-176688023389\"},{\"type\":\"html.InputNode\",\"size\":{\"width\":75,\"height\":75},\"inPorts\":[],\"outPorts\":[\"out\"],\"position\":{\"x\":420,\"y\":100},\"settings\":{\"@class\":\".InputNodeSettings\",\"uploadId\":%d,\"transforms\":[],\"columns\":[]},\"id\":\"fd638363-8161-449e-886d-745d5a737afc\"}]}";

  private void push(TableBookmark tab, BookmarkVizStateChangeEvent event){
    event.setTabId(tab.getId());
    bookmarkStateStorage.add(event);
  }

  @Test
  @Ignore
  public void simpleTest() throws Exception {

    User user = userRepository.saveUser(new User("user", "user"));
    Auth.set(new Auth(user.getId(), ""));

    String fileName = "test_types.xls";
    Upload upload = uploadUtils.createFile(fileName, user.getId(), null, DataFormat.CONTENT_TYPE_XLS);
    List<AbstractFile> children = uploadRepository.getFiles(user.getId(), upload.getId());
    assertEquals(children.size(), 1);

    Datadoc datadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));
    Long sheet1Id = children.get(0).getId();
    String flowJSON = String.format(flowJSONTemplate, datadoc.getId(), sheet1Id);

    taskManagementService.executeSync(new Auth(datadoc.getUser().getId(), null),
        new FlowExecutionRequest(flowJSON, null, null, datadoc.getId(), true, false, false, false));

    TableBookmark bookmark = tableRepository.getTableBookmark(datadoc.getId());
    BookmarkState state = bookmark.getState();

    assertEquals(toShows(state.getColumnList(), "Boolean", "Date", "Double", "Integer", "Location", "Location (Country)", "Location (State)", "String", "Time"),
        state.getQueryParams().getShows());

    push(bookmark, new ShowRemoveEvent(show(state.getColumnList(), "Name")));
    push(bookmark, new ShowRemoveEvent(show(state.getColumnList(), "Date")));
    push(bookmark, new ShowRemoveEvent(show(state.getColumnList(), "Location (Country)")));
    push(bookmark, new ShowRemoveEvent(show(state.getColumnList(), "Location (State)")));
    push(bookmark, new ShowMoveEvent(show(state.getColumnList(), "Time"), 0));
    push(bookmark, new ShowMoveEvent(show(state.getColumnList(), "Double"), 0));
    push(bookmark, new ShowAddEvent(show(state.getColumnList(), "Integer"), 3));
    state = bookmarkStateStorage.get(null, true).getState();
    assertEquals(toShows(state.getColumnList(), "Double", "Time", "Boolean", "Integer", "Location", "String"),
        state.getQueryParams().getShows());

    push(bookmark, new AggAddEvent(agg(state.getColumnList(), "Location"), 0));
    push(bookmark, new AggAddEvent(agg(state.getColumnList(), "String"), 1));
    push(bookmark, new AggAddEvent(agg(state.getColumnList(), "Boolean"), 0));
    push(bookmark, new AggMoveEvent(agg(state.getColumnList(), "String"), 0));
    push(bookmark, new AggRemoveEvent(agg(state.getColumnList(), "String")));
    state = bookmarkStateStorage.get(null, true).getState();
    assertEquals(toAggs(state.getColumnList(), "Boolean", "Location"), state.getQueryParams().getAggs());
  }

  private Show show(List<Col> cols, String name){
    return new Show(cols.stream().filter(c -> c.getName().equals(name)).findFirst().get().getField());
  }

  private Agg agg(List<Col> cols, String name){
    return new Agg(cols.stream().filter(c -> c.getName().equals(name)).findFirst().get().getField());
  }

  private List<Show> toShows(List<Col> cols, String... shows){
    return Arrays.stream(shows).map((name) -> show(cols, name)).collect(Collectors.toList());
  }

  private List<Agg> toAggs(List<Col> cols, String... aggs){
    return Arrays.stream(aggs).map((name) -> agg(cols, name)).collect(Collectors.toList());
  }
}
