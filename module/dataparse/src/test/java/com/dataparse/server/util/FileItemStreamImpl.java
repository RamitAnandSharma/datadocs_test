package com.dataparse.server.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Builder;
import org.apache.commons.fileupload.FileItemHeaders;
import org.apache.commons.fileupload.FileItemStream;

import java.io.IOException;
import java.io.InputStream;

@Data
@Builder
@AllArgsConstructor
public class FileItemStreamImpl implements FileItemStream {

  private String name;
  private String fieldName;
  private String contentType;
  private boolean formField;
  private FileItemHeaders headers;

  public FileItemStreamImpl(String name, String contentType) {
    this.name = name;
    this.contentType = contentType;
  }

  @Override
  public InputStream openStream() throws IOException {
    return this.getClass().getClassLoader().getResourceAsStream(name);
  }

}
