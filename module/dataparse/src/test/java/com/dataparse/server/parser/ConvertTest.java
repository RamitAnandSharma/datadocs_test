package com.dataparse.server.parser;

import com.amazonaws.util.*;
import com.dataparse.server.*;
import com.dataparse.server.auth.*;
import com.dataparse.server.service.flow.convert.*;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.user.*;
import com.dataparse.server.util.*;
import org.junit.*;
import org.springframework.beans.factory.annotation.*;

public class ConvertTest extends IsolatedContextTest {

  @Autowired
  private ConvertService convertService;

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private IStorageStrategy storageStrategy;

  @Autowired
  private FileUploadUtils uploadingUtils;

  @Test
  public void convertJsonToCSVTest() throws Exception {
    User user = userRepository.saveUser(new User("user", "user"));
    Auth.set(new Auth(user.getId(), ""));
    ResourceFileStorage resourceStorage = new ResourceFileStorage();
    String fileName = "test-map.json";
    Upload upload = uploadingUtils.createFile(fileName, user.getId(), null, DataFormat.CONTENT_TYPE_JSON);

    Descriptor converted = convertService.convert(upload.getDescriptor(), StorageType.LOCAL, progress -> {});
    String csvContents = IOUtils.toString(storageStrategy.get(((FileDescriptor) converted)).getFile(((FileDescriptor) converted).getPath()));
    String sampleCsvContents = IOUtils.toString(resourceStorage.getFile("test-map.csv"));
    Assert.assertTrue(sampleCsvContents.equals(csvContents));
  }

}
