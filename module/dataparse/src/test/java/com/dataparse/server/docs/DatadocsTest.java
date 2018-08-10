package com.dataparse.server.docs;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.controllers.api.table.UpdateTableBookmarkRequest;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;

public class DatadocsTest extends IsolatedContextTest {

  @Autowired
  public UserRepository userRepository;

  @Autowired
  public TableService tableService;

  @Autowired
  public TableRepository tableRepository;


  @Test
  public void testBookmark(){
    User user = userRepository.saveUser(new User("user", "user"));
    Auth.set(new Auth(user.getId(), ""));

    String fileName = "tmp";
    Datadoc datadoc = tableService.createDatadoc(new CreateDatadocRequest(fileName));

    assertEquals(fileName, datadoc.getName());
    BookmarkState bookmarkState = new BookmarkState();
    bookmarkState.setBookmarkStateId(new BookmarkStateId(null, UUID.randomUUID(), user.getId()));

    TableBookmark bookmark1 = tableRepository.saveBookmark(tableRepository.initTableBookmark("t1", null), datadoc.getId(), bookmarkState);
    TableBookmark bookmark2 = tableRepository.saveBookmark(tableRepository.initTableBookmark("t2", null), datadoc.getId(), bookmarkState);

    bookmark2 = tableRepository.updateTableBookmark(bookmark2.getId(), new UpdateTableBookmarkRequest("Sheet4"));

    TableBookmark bookmark3 = tableRepository.saveBookmark(tableRepository.initTableBookmark("t3", null), datadoc.getId(), bookmarkState);

    assertEquals("t1", bookmark1.getName());
    assertEquals(1, bookmark1.getPosition());
    assertEquals("Sheet4", bookmark2.getName());
    assertEquals(2, bookmark2.getPosition());
    assertEquals("t3", bookmark3.getName());
    assertEquals(3, bookmark3.getPosition());

    List<TableBookmark> bookmarkList = tableRepository.getTableBookmarks(datadoc.getId());
    assertEquals(4, bookmarkList.size());
    assertEquals("Sheet1", bookmarkList.get(0).getName());
    assertEquals("t1", bookmarkList.get(1).getName());
    assertEquals("Sheet4", bookmarkList.get(2).getName());
    assertEquals("t3", bookmarkList.get(3).getName());

    tableRepository.removeTableBookmark(bookmark2.getId());
    bookmarkList = tableRepository.getTableBookmarks(datadoc.getId());
    assertEquals(3, bookmarkList.size());
    assertEquals("Sheet1", bookmarkList.get(0).getName());
    assertEquals("t1", bookmarkList.get(1).getName());
    assertEquals(1, bookmarkList.get(1).getPosition());
    assertEquals(2, bookmarkList.get(2).getPosition());
  }

  @Test
  public void testMoveBookmark(){
    User user = userRepository.saveUser(new User("user1", "user1"));
    Auth.set(new Auth(user.getId(), ""));
    BookmarkState bookmarkState = new BookmarkState();
    bookmarkState.setBookmarkStateId(new BookmarkStateId(null, UUID.randomUUID(), user.getId()));

    Datadoc datadoc = tableService.createDatadoc(new CreateDatadocRequest("table1"));
    TableBookmark b2 = tableRepository.saveBookmark(tableRepository.initTableBookmark("t2", null), datadoc.getId(), bookmarkState);
    TableBookmark b3 = tableRepository.saveBookmark(tableRepository.initTableBookmark("t3", null), datadoc.getId(), bookmarkState);


    // 3 tabs: 1 2 3
    // move second tab: 1 3 2
    tableRepository.moveTableBookmark(b2.getId(), 2);
    List<TableBookmark> bookmarkList = tableRepository.getTableBookmarks(datadoc.getId());
    assertEquals("Sheet1", bookmarkList.get(0).getName());
    assertEquals("t3", bookmarkList.get(1).getName());
    assertEquals("t2", bookmarkList.get(2).getName());

    // 3 tabs: 1 2 3
    // move third tab: 3 1 2
    tableRepository.moveTableBookmark(b3.getId(), 0);
    bookmarkList = tableRepository.getTableBookmarks(datadoc.getId());
    assertEquals("t3", bookmarkList.get(0).getName());
    assertEquals("Sheet1", bookmarkList.get(1).getName());
    assertEquals("t2", bookmarkList.get(2).getName());
  }
}
