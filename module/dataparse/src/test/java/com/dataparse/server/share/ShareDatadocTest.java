package com.dataparse.server.share;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.share.ShareDatadocInfo;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.ShareType;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.share.ShareRepository;
import com.dataparse.server.service.share.ShareService;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class ShareDatadocTest extends IsolatedContextTest {

    @Autowired
    private ShareService shareService;
    @Autowired
    private ShareRepository shareRepository;
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private TableService tableService;

    @Test
    public void shareDatadocTest() {
        User user = userRepository.saveUser(new User("user", "user"));
        User userToShare = userRepository.saveUser(new User("userToShare", "user"));
        Auth.set(new Auth(user.getId(), ""));

        Datadoc datadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));
        shareService.shareDatadoc(datadoc, userToShare.getEmail(), ShareType.VIEW, false);

        List<Datadoc> sharedDatadocs = shareRepository.getSharedDatadocs(userToShare.getId());
        Assert.assertEquals(sharedDatadocs.size(), 1);
    }

    @Test
    public void shareDatadocRecursiveTest() {
        User userRoot = userRepository.saveUser(new User("userRoot", "user"));
        User userToShare11 = userRepository.saveUser(new User("userToShare11", "user"));
        User userToShare12 = userRepository.saveUser(new User("userToShare12", "user"));
        User userToShare21 = userRepository.saveUser(new User("userToShare21", "user"));
        User userToShare31 = userRepository.saveUser(new User("userToShare31", "user"));

        setCurrentUser(userRoot);
        Datadoc datadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));

//        second graph layer
        shareService.shareDatadoc(datadoc, userToShare11.getEmail(), ShareType.ADMIN, false);
        shareService.shareDatadoc(datadoc, userToShare12.getEmail(), ShareType.ADMIN, false);
// third graph layer
        setCurrentUser(userToShare11);
        shareService.shareDatadoc(datadoc, userToShare21.getEmail(), ShareType.ADMIN, false);
        shareService.shareDatadoc(datadoc, userToShare31.getEmail(), ShareType.ADMIN, false);
        setCurrentUser(userRoot);

        ShareDatadocInfo shareDatadocInfo = shareService.retrieveShareDatadocInfo(datadoc.getId());
        Assert.assertEquals(shareDatadocInfo.getSharedWith().size(), 4);
        shareService.revokePermissions(datadoc.getId(), userToShare21.getId());
        shareDatadocInfo = shareService.retrieveShareDatadocInfo(datadoc.getId());
        Assert.assertEquals(shareDatadocInfo.getSharedWith().size(), 3);
    }

    @Test
    public void shareAvailabilityTest() {
        User u1 = userRepository.saveUser(new User("David", "user"));
        User u2 = userRepository.saveUser(new User("Vlad", "user"));

        setCurrentUser(u1);
        Datadoc mockDatadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));
        Datadoc secondDatadoc = tableService.createDatadoc(new CreateDatadocRequest("tmp"));
        List<User> shareWithUsers = shareRepository.getShareWithUsers(mockDatadoc.getId(), u1.getId(), null);
        Assert.assertEquals(shareWithUsers.size(), 0);

        shareService.shareDatadoc(mockDatadoc, u2.getEmail(), ShareType.ADMIN, false);

        shareWithUsers = shareRepository.getShareWithUsers(secondDatadoc.getId(), u1.getId(), null);
        Assert.assertEquals(shareWithUsers.size(), 1);
        shareWithUsers = shareRepository.getShareWithUsers(secondDatadoc.getId(), u1.getId(), "vla");
        Assert.assertEquals(shareWithUsers.size(), 1);
        shareWithUsers = shareRepository.getShareWithUsers(secondDatadoc.getId(), u1.getId(), "Vla");
        Assert.assertEquals(shareWithUsers.size(), 1);
        shareWithUsers = shareRepository.getShareWithUsers(secondDatadoc.getId(), u1.getId(), "some");
        Assert.assertEquals(shareWithUsers.size(), 0);

    }

    private void setCurrentUser(User user) {
        Auth.set(new Auth(user.getId(), ""));
    }

}
