package com.dataparse.server.coverage.utils.helpers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.file.Files.probeContentType;
import static java.util.Objects.requireNonNull;


public class FileList {

  final static public String mocksDirectory = "coverage/mocks";

  static public String getMockPath(String testName, String fileName) {
    return String.format("%s/%s/%s", mocksDirectory, testName, fileName);
  }

  static public InputStream getResourceFileStream(String testName, String fileName) {
    String filePath = String.format("%s/%s/%s", mocksDirectory, testName, fileName);
    return FileList.class.getClassLoader().getResourceAsStream(filePath);
  }

  static public String getFileContentType(String testName, String fileName) throws URISyntaxException, IOException {
    return probeContentType(Paths.get(FileList.class.getClassLoader().getResource(FileList.getMockPath(testName, fileName)).toURI()));
  }

  static public List<String> getTestMockFiles(String testName) {
    try {
      return Arrays.stream(
          requireNonNull(new File(requireNonNull(FileList.class.getClassLoader().getResource("coverage/mocks/" + testName)).toURI()).listFiles())
          )
          .filter(File::isFile)
          .map(File::getName)
          .collect(Collectors.toList());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return new ArrayList<>();
  }

}
