package com.dataparse.server.cleanup;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.service.cleanup.CleanUpService;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class CleanUpTest extends IsolatedContextTest {

  @Autowired
  private CleanUpService cleanUpService;

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private TableService tableService;

  @Autowired
  private SessionFactory sessionFactory;

  @Test
  public void cleanDatadocsTest() {
    User user = userRepository.saveUser(new User("user", "user"));
    Auth.set(new Auth(user.getId(), ""));
    String datadocForRemove = "datadocForRemove";
    String savedDatadoc = "savedDatadoc";

    tableService.createDatadoc(new CreateDatadocRequest(datadocForRemove, null, null, true, false, null,false));
    tableService.createDatadoc(new CreateDatadocRequest(savedDatadoc, null, null, false, false, null, false));
    DateTime twoDaysAgo = new DateTime().minusDays(2);
    Session session = sessionFactory.openSession();
    session.beginTransaction();
    int countOfChanges = session.createQuery("UPDATE AbstractFile set created = :date where name = :name")
        .setParameter("date", twoDaysAgo.toDate())
        .setParameter("name", datadocForRemove)
        .executeUpdate();
    session.getTransaction().commit();
    Assert.assertEquals(countOfChanges, 1);

    cleanUpService.runCleanUp();

    List files = session.createQuery("SELECT f FROM AbstractFile f where deleted = :false").setParameter("false", false).list();
    Assert.assertEquals(files.size(), 1);
    AbstractFile datadoc = (AbstractFile) files.get(0);
    Assert.assertEquals(datadoc.getName(), savedDatadoc);

    files = session.createQuery("SELECT f FROM AbstractFile f where deleted = :true").setParameter("true", true).list();
    Assert.assertEquals(files.size(), 1);
    datadoc = (AbstractFile) files.get(0);
    Assert.assertEquals(datadoc.getName(), datadocForRemove);


  }

}
