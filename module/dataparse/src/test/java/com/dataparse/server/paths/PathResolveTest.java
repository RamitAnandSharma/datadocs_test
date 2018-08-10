package com.dataparse.server.paths;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.upload.UploadRepository;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.util.TestUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

public class PathResolveTest extends IsolatedContextTest {

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private UserRepository userRepository;

    @Test
    public void resolveTest(){

        User user = userRepository.saveUser(new User("user", "user"));
        Auth.set(new Auth(user.getId(), ""));

        Long folder1 = uploadRepository.createFolder(null, "folder1").getId();
        Long folder2 = uploadRepository.createFolder(null, "folder2").getId();

        Long child1 = uploadRepository.createFolder(folder1, "child1").getId();
        Long child2 = uploadRepository.createFolder(folder1, "child2").getId();

        assertNull(uploadRepository.resolveFileIdByPath(""));
        assertNull(uploadRepository.resolveFileIdByPath("/"));
        assertEquals(folder1, uploadRepository.resolveFileIdByPath("/folder1"));
        assertEquals(folder2, uploadRepository.resolveFileIdByPath("/folder2"));
        assertEquals(folder1, uploadRepository.resolveFileIdByPath("/folder1/"));
        assertEquals(child1, uploadRepository.resolveFileIdByPath("/folder1/child1"));
        assertEquals(child2, uploadRepository.resolveFileIdByPath("/folder1/child2"));
        assertTrue(TestUtils.isExceptionThrown(RuntimeException.class, () -> uploadRepository.resolveFileIdByPath("/folder1/child3")));

    }
}
