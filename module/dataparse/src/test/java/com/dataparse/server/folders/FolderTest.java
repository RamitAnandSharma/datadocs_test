package com.dataparse.server.folders;

import com.dataparse.server.IsolatedContextTest;
import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.FileController;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.Folder;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.util.FileUploadUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;

@Slf4j
public class FolderTest extends IsolatedContextTest {

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    FileController fileController;

    @Autowired
    private UploadService uploadService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private FileUploadUtils uploadUtils;

    @Test
    public void testNested() throws Exception{

        User user = userRepository.saveUser(new User("user", "user"));
        String fileName = "test.xls";

        Auth.set(new Auth(user.getId(), ""));

        Upload upload = uploadUtils.createFile(fileName, user.getId(), null, DataFormat.CONTENT_TYPE_XLS);

        assertEquals(uploadRepository.getFiles(user.getId(), null).size(), 1);
        List<AbstractFile> children = uploadRepository.getFiles(user.getId(), upload.getId());
        assertEquals(children.size(), 3);

        Folder folder1 = uploadRepository.createFolder(null, "folder1");
        assertEquals(uploadRepository.getFiles(user.getId(), null).size(), 2);

        Folder folder2 = uploadRepository.createFolder(folder1.getId(), "folder2");
        assertEquals(uploadRepository.getFiles(user.getId(), folder1.getId()).size(), 1);

        uploadRepository.moveToFolder(upload.getId(), folder1.getId());
        assertEquals(uploadRepository.getFiles(user.getId(), folder1.getId()).size(), 2);

        uploadRepository.moveToFolder(upload.getId(), folder2.getId());
        assertEquals(uploadRepository.getFiles(user.getId(), folder1.getId()).size(), 1);
        assertEquals(uploadRepository.getFiles(user.getId(), folder2.getId()).size(), 1);
    }

    @Test
    public void testDuplicateNames() throws Exception{
        User user = userRepository.saveUser(new User("user2", "user2"));
        Auth.set(new Auth(user.getId(), ""));

        Folder folder1 = uploadRepository.createFolder(null, "folder");
        assertEquals(folder1.getName(), "folder");
        Folder folder2 = uploadRepository.createFolder(null, "folder");
        assertEquals(folder2.getName(), "folder (1)");

        Folder folder3 = uploadRepository.createFolder(folder1.getId(), "folder");
        assertEquals(folder3.getName(), "folder");
        Folder folder4 = uploadRepository.createFolder(folder1.getId(), "folder");
        assertEquals(folder4.getName(), "folder (1)");
        Folder folder5 = uploadRepository.createFolder(folder1.getId(), "folder");
        assertEquals(folder5.getName(), "folder (2)");

        uploadService.removeFile(folder4.getId());
        folder4 = uploadRepository.createFolder(folder1.getId(), "folder");
        assertEquals(folder4.getName(), "folder (1)");
        Folder folder6 = uploadRepository.createFolder(folder1.getId(), "folder");
        assertEquals(folder6.getName(), "folder (3)");

        uploadService.removeFile(folder1.getId());
        uploadService.removeFile(folder2.getId());
    }
}
