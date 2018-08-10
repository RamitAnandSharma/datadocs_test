package com.dataparse.server.share;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.share.ShareRepository;
import com.dataparse.server.service.share.ShareService;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

@ContextConfiguration
@EnableCaching
@Slf4j
public class ShareWithSpringCacheTest extends IsolatedContextTest {

    @Autowired
    private ShareService shareService;

    @Autowired
    private ShareRepository shareRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TableService tableService;

    // Todo: Maybe improve it with Mockito?
    @Test
    public void cacheTest() {
        User owner = userRepository.saveUser(new User("user", "user"));
        User testUser = userRepository.saveUser(new User("userToShare", "user"));
        setCurrentUser(owner);

        Datadoc datadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));
        shareService.shareDatadoc(datadoc, testUser.getEmail(), ShareType.VIEW, false);

        List<Datadoc> sharedDatadocs = shareRepository.getSharedDatadocs(testUser.getId());
        Assert.assertEquals(1, sharedDatadocs.size());

        setCurrentUser(testUser);
        Datadoc testDatadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));

        log.info("Users & Datadocs were created. Starting the main part of the test...");

        List<User> shareWithUsers1 = shareRepository.getShareWithUsers(testDatadoc.getId(), testUser.getId(), "us");
        Assert.assertEquals(1, shareWithUsers1.size());
        log.info("The result of the first method call: {}", shareWithUsers1);
        List<User> shareWithUsers2 = shareRepository.getShareWithUsers(testDatadoc.getId(), testUser.getId(), "us");
        log.info("The result of the first method call: {}", shareWithUsers2);
        Assert.assertEquals(shareWithUsers1, shareWithUsers2);
    }

    private void setCurrentUser(User user) {
        Auth.set(new Auth(user.getId(), ""));
    }

}
