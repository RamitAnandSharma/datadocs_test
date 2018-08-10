package com.dataparse.server.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataparse.server.controllers.api.file.UploadFileRequest;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadService;

@Service
public class FileUploadUtils {
  @Autowired
  private UploadService uploadService;
  
  public UploadFileRequest buildUploadFileRequest(String path) {
    return new UploadFileRequest(path, 100L);
  }

  public Upload createFile(String fileName, Long userId, Long parentId, String contentType) throws Exception {
    UploadFileRequest uploadFileRequest = buildUploadFileRequest(fileName);
    Long descriptorId = uploadService.saveFile(uploadFileRequest, new FileItemStreamImpl(fileName, contentType));
    return (Upload) uploadService.createFile(userId, descriptorId, parentId);
  }
}
