package com.dataparse.server.service.upload;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.file.*;
import com.dataparse.server.controllers.exception.ResourceAlreadyExists;
import com.dataparse.server.service.common.CancellationRequestService;
import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.db.DBService;
import com.dataparse.server.service.db.FileParams;
import com.dataparse.server.service.db.TableInfo;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.entity.StickyNoteType;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.File;
import com.dataparse.server.service.files.Folder;
import com.dataparse.server.service.files.preview.FilePreviewService;
import com.dataparse.server.service.flow.cache.FlowPreviewResultCache;
import com.dataparse.server.service.mail.DisconnectedSourceEmail;
import com.dataparse.server.service.mail.MailService;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.exception.TooLargeFileException;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.share.ShareService;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.storage.StorageSelectionStrategy;
import com.dataparse.server.service.storage.StorageType;
import com.dataparse.server.service.storage.unifersal.ISignedUrlStorage;
import com.dataparse.server.service.tasks.ExceptionWrapper;
import com.dataparse.server.service.tasks.ExecutionException;
import com.dataparse.server.service.upload.preprocess.DescriptorFactory;
import com.dataparse.server.service.upload.preprocess.IInflateArchive;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkStateRepository;
import com.dataparse.server.util.*;
import com.dataparse.server.util.db.DbConnectionProvider;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.HibernateException;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

import static com.dataparse.server.util.FunctionUtils.complement;


@Slf4j
@Service
@EnableScheduling
// todo make it simple
public class UploadService {

  @Autowired
  private MailService mailService;

  @Autowired
  private FlowPreviewResultCache flowPreviewResultCache;

  @Autowired
  private IInflateArchive inflateArchive;

  @Autowired
  private BookmarkStateRepository bookmarkStateRepository;

  @Autowired
  private DBService dbService;

  @Autowired
  private UploadRepository uploadRepository;

  @Autowired
  private StorageSelectionStrategy storageSelectionStrategy;

  @Autowired
  private CancellationRequestService cancelationService;

  @Autowired
  private IStorageStrategy storageStrategy;

  @Autowired
  private ParserFactory parserFactory;

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private TableService tableService;

  @Autowired
  private ShareService shareService;

  @Autowired
  private FilePreviewService filePreviewService;

  @Autowired
  private DbConnectionProvider dbConnectionProvider;

  @Autowired
  @Qualifier("GOOGLE_DRIVE")
  private ISignedUrlStorage signedUrlService;

  public List<PrepareUploadResponse> prepareUpload(PrepareUploadRequest request) {
    Map<String, String> fileUrls = signedUrlService.getSignedUploadUrls(request.getCount());
    return fileUrls.entrySet().stream().map(session -> {
      String fileName = session.getKey();
      String accessToken = session.getValue();
      UploadSession uploadSession = uploadRepository.createUploadSession(fileName);
      return new PrepareUploadResponse(uploadSession.getId().toString(), accessToken);
    }).collect(Collectors.toList());
  }

  public ConfirmUploadResponse confirmUpload(ConfirmUploadRequest request) {
    String checksum = request.getHash();
    checkFileExisting(checksum);
    StorageType storageType = StorageType.GD;

    DataFormat dataFormat = tryToGuessFileFormat(storageStrategy.get(storageType), request.getFileName(), request.getContentType(), request.getFileId());
    if (dataFormat == null || DataFormat.JSON_OBJECT.equals(dataFormat)) {
      dataFormat = tryDefineFormatByFileContents(storageType, request.getFileId());
    }

    if (inflateArchive.formatSupported(dataFormat)) {

      String newFileId = UUID.randomUUID().toString();

      try (InputStream archive = storageStrategy.get(storageType).getFile(request.getFileId())) {
        if (inflateArchive.getFilesCount(archive, dataFormat) != 1) {
          throw new UnsupportedOperationException("Only archives with one file are currently supported.");
        }
      } catch (IOException e) {
        throw new RuntimeException("Can not download file " + dataFormat, e);
      }

      try {
        InputStream archive = storageStrategy.get(storageType).getFile(request.getFileId());
        ZipEntry zipEntry = inflateArchive.getContainedFiles(archive, dataFormat).get(0);

        archive = storageStrategy.get(storageType).getFile(request.getFileId());
        InputStream inflatedStream = inflateArchive.inflate(archive, dataFormat, zipEntry);

        copyFileToGCS(inflatedStream, newFileId);
        storageStrategy.get(StorageType.GD).removeFile(request.getFileId());

        String newRealFileName = StringUtils.isBlank(zipEntry.getName())
            ? request.getFileName().substring(0, request.getFileName().lastIndexOf('.'))
                : zipEntry.getName();

            request.setFileId(newFileId);
            request.setFileName(newRealFileName);
            request.setContentType("");
            storageType = StorageType.GCS;
            dataFormat = tryToGuessFileFormat(storageStrategy.get(storageType), request.getFileName(), request.getContentType(), request.getFileId());
      } catch (IOException e) {
        throw new RuntimeException("Can not inflate archive by type " + dataFormat, e);
      }
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    UploadSession uploadSession = uploadRepository.getUploadSession(request.getSessionId());

    if (uploadSession == null || !uploadSession.getUser().getId().equals(Auth.get().getUserId())) {
      throw new RuntimeException("Upload session not found.");
    }

    Long parentId = uploadRepository.resolveFileIdByPath(request.getPath());

    try {
      Long descriptorId = registerFile(request, dataFormat, checksum, request.getFileId(), storageType,
          IOUtils.toInputStream(""));
      File file = createFile(Auth.get().getUserId(), descriptorId, parentId);
      log.info("Confirming file {} took {}", file.getName(), stopwatch.stop());
      uploadRepository.deleteUploadSession(uploadSession);
      return new ConfirmUploadResponse(file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void copyFileToGCS(String fileId, String resultFileName) {
    InputStream fileStream = signedUrlService.getFile(fileId);
    copyFileToGCS(fileStream, resultFileName);
  }

  public void copyFileToGCS(InputStream inputStream, String resultFileName) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      FileStorage fileStorage = storageStrategy.get(StorageType.GCS);
      fileStorage.saveFile(inputStream, resultFileName);
      log.info("Copy file {} to GCS took {}", resultFileName, stopwatch.stop());
    } catch (IOException e) {
      throw new RuntimeException("Can not copy file from GD to GCS. ", e);
    }
  }

  public DataFormat tryDefineFormatByFileContents(StorageType type, String path) {

    // try to guess upload format by looking into file contents
    for (DataFormat dataFormat : DataFormat.values()) {
      try {
        if (dataFormat.testContents(storageStrategy.get(type), path)) {
          return dataFormat;
        }
      } catch (Exception e) {
        continue;
      }
    }

    return DataFormat.UNDEFINED;
  }

  public AbstractFile refreshTables(Long id) {
    AbstractFile file = uploadRepository.getFile(id, true, true, false);
    if (file instanceof Upload && ((Upload) file).getDescriptor() instanceof DbDescriptor) {
      Upload upload = (Upload) file;
      Map<String, Upload> existsTables = upload.getSections().stream().filter(section -> section.getDescriptor() instanceof DbTableDescriptor).collect(Collectors.toMap(Upload::getName, v -> v));
      ConnectionParams params = ((DbDescriptor) upload.getDescriptor()).getParams();
      Stopwatch stopwatch = Stopwatch.createStarted();
      Map<String, TableInfo> tablesByName = dbService.getTables(params, true).stream().collect(Collectors.toMap(TableInfo::getName, v -> v));
      log.info("Retrieve {} tables for refresh took {}", tablesByName.size(), stopwatch.stop());

      Sets.SetView<String> newTables = Sets.difference(tablesByName.keySet(), existsTables.keySet());
      Sets.SetView<String> tablesForRemove = Sets.difference(existsTables.keySet(), tablesByName.keySet());
      if (newTables.size() > 0) {
        List<Upload> uploads = newTables.stream().map(table -> {
          TableInfo tableInfo = tablesByName.get(table);
          return createTable(Auth.get().getUserId(), tableInfo, params, upload);
        }).collect(Collectors.toList());
        uploadRepository.saveFiles(uploads);
      }
      if (tablesForRemove.size() > 0) {
        List<Long> tableForRemoveIds = tablesForRemove.stream().map(table -> existsTables.get(table).getId()).collect(Collectors.toList());
        uploadRepository.deleteUploads(tableForRemoveIds);
      }
      if (newTables.size() == 0 && tablesForRemove.size() == 0) {
        return file;
      } else {
        return uploadRepository.getFile(id, true, true, false);
      }
    }
    return file;
  }

  public void updateRowsCount(Collection<Upload> existsTables, Map<String, TableInfo> updatedInfoByTableName) {
    List<Descriptor> descriptorsForUpdate = existsTables.stream().map(table -> {
      Descriptor descriptor = table.getDescriptor();
      TableInfo tableInfo = updatedInfoByTableName.get(table.getName());
      if (tableInfo != null && !tableInfo.getRowsCount().equals(descriptor.getRowsCount())) {
        descriptor.setRowsEstimatedCount(tableInfo.getRowsCount());
        return descriptor;
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
    if (descriptorsForUpdate.size() > 0) {
      log.info("Updating rows count for {} tables. ", descriptorsForUpdate.size());
      uploadRepository.updateDescriptors(descriptorsForUpdate);
    }
  }

  public Upload createTable(Long userId, TableInfo table, ConnectionParams connectionParams, Upload parent) {
    DataFormat dataFormat;
    switch (connectionParams.getProtocol()) {
    case mysql:
      dataFormat = DataFormat.MYSQL_TABLE;
      break;
    case postgresql:
      dataFormat = DataFormat.POSTGRESQL_TABLE;
      break;
    case sqlserver:
      dataFormat = DataFormat.MSSQL_TABLE;
      break;
    case oracle:
      dataFormat = DataFormat.ORACLE_TABLE;
      break;
    default:
      throw new RuntimeException("Unknown protocol: " + connectionParams.getProtocol());
    }

    DbTableDescriptor tableDescriptor = new DbTableDescriptor();
    tableDescriptor.setFormat(dataFormat);
    tableDescriptor.setTableName(table.getName());
    tableDescriptor.setParams(connectionParams);
    tableDescriptor.setValid(true);
    tableDescriptor.setLastConnected(new Date());
    tableDescriptor.setErrorCode(null);
    tableDescriptor.setErrorString(null);
    tableDescriptor.setColumns(table.getColumns());
    tableDescriptor.setRowsEstimatedCount(table.getRowsCount());

    Upload upload = new Upload();
    upload.setName(table.getName());
    upload.setDescriptor(tableDescriptor);
    upload.setUploaded(new Date());
    upload.setParent(parent);
    upload.setUser(userRepository.getUser(userId));
    return upload;
  }

  /**
   * @param uuid - is file in storage.
   */
  @Transactional
  public boolean deleteFileByUuid(String uuid) {
    try {
      List<File> files = uploadRepository.getFilesByUuid(uuid);
      if (files.size() > 0) {
        List<Long> uploadIds = files.stream().map(File::getId).collect(Collectors.toList());
        uploadIds.forEach(id -> uploadRepository.deleteUpload(id));
        removeFiles(uploadIds);
        return true;
      }
      return false;
    } catch (HibernateException e) {
      log.debug("Failed to remove file by uuid.", e);
      return false;
    }
  }

  private Upload updateQueryParams(Upload queryUpload) {
    queryUpload.getDescriptor().setLimit(1000);
    try (Parser parser = parserFactory.getParser(queryUpload.getDescriptor());
        RecordIterator it = parser.parse()) {
      Pair<Long, Boolean> rowCount = parser.getRowsEstimateCount(0);
      if (!it.hasNext()) {
        throw new RuntimeException("Query results are empty!");
      }
      if (rowCount.getRight()) {
        queryUpload.getDescriptor().setRowsEstimatedCount(null);
        queryUpload.getDescriptor().setRowsExactCount(rowCount.getLeft());
      } else {
        queryUpload.getDescriptor().setRowsEstimatedCount(rowCount.getLeft());
        queryUpload.getDescriptor().setRowsExactCount(null);
      }
      queryUpload.getDescriptor().setColumns(tryParseColumns(parser));
      queryUpload.getDescriptor().setLimit(null);
      return queryUpload;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Upload editQueryOfRemoteDataSource(Long queryId, UpdateRemoteQueryRequest request) {
    AbstractFile obj = uploadRepository.getFile(queryId);
    // todo simplify?
    if (obj == null || !(obj instanceof Upload) || !(((Upload) obj).getDescriptor() instanceof DbQueryDescriptor)) {
      throw new RuntimeException("Remote data source not found");
    }
    Upload query = (Upload) obj;
    DbQueryDescriptor queryDescriptor = (DbQueryDescriptor) query.getDescriptor();
    query.setName(request.getName());
    queryDescriptor.setQuery(request.getQuery());
    queryDescriptor.setErrorCode(null);
    queryDescriptor.setErrorString(null);
    queryDescriptor.setLastConnected(new Date());
    queryDescriptor.setValid(true);
    updateQueryParams(query);
    uploadRepository.updateFile(query);
    return query;
  }

  public Upload testQueryOfRemoteDataSource(String query, ConnectionParams connectionParams) {
    DbQueryDescriptor descriptor = new DbQueryDescriptor();
    descriptor.setParams(connectionParams);
    descriptor.setQuery(query);
    descriptor.setComposite(false);
    descriptor.setFormat(DbUtils.getRemoteDataSourceFormat(connectionParams.getProtocol()));

    Upload queryUpload = new Upload();
    queryUpload.setDescriptor(descriptor);

    updateQueryParams(queryUpload);
    return queryUpload;
  }

  public Upload testConnectionOfRemoteDataSource(Long linkId, TestConnectionRequest connectionRequest) {
    ConnectionParams params = connectionRequest.getConnectionParams();
    AbstractFile obj = uploadRepository.getFile(linkId, false, true, false);
    // todo simplify?
    if (obj == null || !(obj instanceof Upload) || !(((Upload) obj).getDescriptor() instanceof DbDescriptor)) {
      throw new RuntimeException("Remote link not found!");
    }
    Upload upload = (Upload) obj;
    DbDescriptor descriptor = (DbDescriptor) upload.getDescriptor();

    try {
      descriptor.setLastConnectionTestSuccessful(dbService.test(params));
      descriptor.setErrorCode(null);
      descriptor.setDisconnectedTime(null);
      descriptor.setErrorString(null);
      descriptor.setValid(true);
    } catch (ExecutionException e) {
      descriptor.setValid(false);
      descriptor.setErrorString(e.getMessage());
      descriptor.setErrorCode(e.getFirstError().getCode());
      if (descriptor.getDisconnectedTime() == null) {
        descriptor.setDisconnectedTime(new Date());
      }
      descriptor.setLastConnectionTestSuccessful(false);
    } finally {
      descriptor.setLastConnected(new Date());
    }
    uploadRepository.updateDescriptor(descriptor);
    uploadRepository.updateFile(upload);
    return (Upload) uploadRepository.getFile(linkId);
  }

  public Upload addQueryToRemoteDataSource(Long userId, Long uploadId, String name, String query) {
    Upload dbUpload = (Upload) uploadRepository.getFile(uploadId, false, true, false);
    if (dbUpload.getDescriptor() instanceof DbDescriptor) {

      DbDescriptor dbDescriptor = (DbDescriptor) dbUpload.getDescriptor();
      DataFormat dataFormat = DbUtils.getRemoteDataSourceFormat(dbDescriptor.getParams().getProtocol());
      for (Upload u : dbUpload.getSections()) {
        if (u.getDescriptor().getFormat().equals(dataFormat)
            && u.getName().equals(name)) {
          throw new RuntimeException("Query with such name already exists");
        }
      }

      DbQueryDescriptor descriptor = new DbQueryDescriptor();
      descriptor.setParams(dbDescriptor.getParams());
      descriptor.setQuery(query);
      descriptor.setComposite(false);
      descriptor.setFormat(dataFormat);

      Upload queryUpload = new Upload();
      queryUpload.setName(name);
      queryUpload.setDescriptor(descriptor);
      queryUpload.setUser(userRepository.getUser(userId));
      updateQueryParams(queryUpload);
      queryUpload.setParent(dbUpload);
      uploadRepository.saveFile(queryUpload);
      return queryUpload;
    } else {
      throw new RuntimeException("Query can only be added to DB datasource!");
    }
  }

  public Upload createRemoteLink(Long userId, Long parentId, CreateRemoteLinkRequest request) {
    Upload upload = new Upload();
    Boolean canceled = false;
    try {
      ConnectionParams connectionParams = request.getConnectionParams();
      List<String> includeTables = request.getIncludeTables();

      DataFormat dataFormat = getFormatByProtocol(connectionParams);
      long start = System.currentTimeMillis();
      // do test connection
      List<TableInfo> tables = dbService.getTables(connectionParams);
      log.info("Retrieve tables took {}ms, for db {}", System.currentTimeMillis() - start, connectionParams.getDbName());
      DbDescriptor descriptor = new DbDescriptor();
      descriptor.setFormat(dataFormat);
      descriptor.setParams(connectionParams);

      descriptor.setValid(true);
      descriptor.setLastConnectionTestSuccessful(true);
      descriptor.setLastConnected(new Date());

      upload.setName(connectionParams.getDbName());
      upload.setDescriptor(descriptor);
      upload.setUploaded(new Date());
      upload.setUser(userRepository.getUser(userId));

      List<Upload> uploads = new ArrayList<>();
      for (TableInfo table : tables) {
        if (!includeTables.isEmpty() && !includeTables.contains(table.getName())) {
          continue;
        }
        uploads.add(createTable(userId, table, connectionParams, upload));
      }

      if (parentId != null) {
        Folder folder = (Folder) uploadRepository.getFile(parentId);
        upload.setParent(folder);
      }

      upload.setSections(uploads);
    } finally {
      canceled = cancelationService.checkCanceledAndRemove(Auth.get().getUserId(), request);
    }

    if(canceled) {
      log.info("Create remote link, has been canceled, for {}", request.getPath());
      return null;
    }
    uploadRepository.saveFile(upload);
    return upload;
  }

  private DataFormat getFormatByProtocol(ConnectionParams connectionParams) {
    switch (connectionParams.getProtocol()) {
    case mysql:
      return DataFormat.MYSQL;
    case postgresql:
      return DataFormat.POSTGRESQL;
    case sqlserver:
      return DataFormat.MSSQL;
    case oracle:
      return DataFormat.ORACLE;
    default:
      throw new RuntimeException("Unknown protocol: " + connectionParams.getProtocol());
    }
  }

  public Upload updateRemoteLink(Long userId, Long linkId, UpdateRemoteLinkRequest request) {
    Boolean canceled = false;
    List<Upload> newUploads;
    Upload upload;
    try {
      ConnectionParams newConnectionParams = request.getConnectionParams();
      List<String> includeTables = request.getIncludeTables();
      FileParams newFileParams = request.getFileParams();

      AbstractFile obj = uploadRepository.getFile(linkId, false, true, false);
      // todo simplify?
      if (obj == null || !(obj instanceof Upload) || !(((Upload) obj).getDescriptor() instanceof DbDescriptor)) {
        throw new RuntimeException("Remote link not found!");
      }
      upload = (Upload) obj;
      DbDescriptor descriptor = (DbDescriptor) upload.getDescriptor();
      ConnectionParams connectionParams = descriptor.getParams();

      List<TableInfo> tables = dbService.getTables(newConnectionParams);
      BeanUtils.copyProperties(newConnectionParams, connectionParams);
      descriptor.setLastConnected(new Date());
      descriptor.setValid(true);
      descriptor.setLastConnectionTestSuccessful(true);
      descriptor.setErrorCode(null);
      descriptor.setErrorString(null);

      Set<String> existingTables = upload.getSections().stream()
          .map(t -> t.getName())
          .collect(Collectors.toSet());

      removeFiles(upload.getSections().stream()
          .filter(u -> !(u.getDescriptor() instanceof DbQueryDescriptor)
              && !includeTables.isEmpty() && !includeTables.contains(u.getName()))
          .map(u -> u.getId())
          .collect(Collectors.toList()));

      Upload finalUpload = upload;
      newUploads = tables.stream()
          .filter(table -> !existingTables.contains(table.getName()) && (includeTables.isEmpty() || includeTables.contains(table.getName())))
          .map(table -> createTable(userId, table, newConnectionParams, finalUpload))
          .collect(Collectors.toList());

      if (newFileParams != null) {
        upload.setName(newFileParams.getName());
      }
    } finally {
      canceled = cancelationService.checkCanceledAndRemove(Auth.get().getUserId(), request);
    }

    if(canceled) {
      log.info("Update remote link, has been canceled, for {}", request.getPath());
      return null;
    }
    uploadRepository.saveFiles(newUploads);
    uploadRepository.updateFile(upload);
    return (Upload) uploadRepository.getFile(linkId);
  }

  public Upload updateSettings(Long id, UpdateSettingsRequest request) {
    Upload upload = (Upload) uploadRepository.getFile(id, false, false, true);
    if (upload == null) {
      throw new RuntimeException("Upload not found!");
    }
    FormatSettings settings = request.getSettings();
    DataFormat format = upload.getDescriptor().getFormat();
    switch (format) {
    case CSV:
      if (!(settings instanceof CsvFormatSettings)) {
        throw new RuntimeException("No matching settings found, expected " + CsvFormatSettings.class.getSimpleName());
      }
      CsvFileDescriptor csvDescriptor = (CsvFileDescriptor) upload.getDescriptor();
      csvDescriptor.setSettings((CsvFormatSettings) settings);
      break;
    case XLS_SHEET:
    case XLSX_SHEET:
      if (!(settings instanceof XlsFormatSettings)) {
        throw new RuntimeException("No matching settings found, expected " + XlsFormatSettings.class.getSimpleName());
      }
      XlsFileDescriptor xlsDescriptor = (XlsFileDescriptor) upload.getDescriptor();
      xlsDescriptor.setSettings((XlsFormatSettings) settings);
      break;
    case XML:
      if (!(settings instanceof XmlFormatSettings)) {
        throw new RuntimeException("No matching settings found, expected " + XmlFormatSettings.class.getSimpleName());
      }
      XmlFileDescriptor xmlDescriptor = (XmlFileDescriptor) upload.getDescriptor();
      xmlDescriptor.setSettings((XmlFormatSettings) settings);
      break;
    default:
      throw new RuntimeException("Settings are not supported for format: " + format.name());
    }
    List<Long> bookmarkIds = bookmarkStateRepository.getBookmarkIdsBySourceId(id);
    flowPreviewResultCache.evict(bookmarkIds);
    preprocessUpload(upload, true);
    uploadRepository.updateFile(upload);
    return upload;
  }

  public PreviewResponse previewData(Long id, PreviewRequest request) {
    Upload upload = (Upload) uploadRepository.getFile(id);
    if (upload == null) {
      throw new RuntimeException("Upload not found!");
    }
    return previewData(request, upload.getDescriptor());
  }

  public PreviewResponse previewData(PreviewRequest request, Descriptor descriptor) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    if (descriptor != null) {
      long count = 0;
      if (request.getSettings() != null) {
        switch (descriptor.getFormat()) {
        case CSV:
          CsvFileDescriptor csvDescriptor = (CsvFileDescriptor) descriptor;
          csvDescriptor.setSettings((CsvFormatSettings) request.getSettings());
          break;
        case XLS_SHEET:
        case XLSX_SHEET:
          XlsFileDescriptor xlsDescriptor = (XlsFileDescriptor) descriptor;
          xlsDescriptor.setSettings((XlsFormatSettings) request.getSettings());
          break;
        case XML:
          XmlFileDescriptor xmlDescriptor = (XmlFileDescriptor) descriptor;
          xmlDescriptor.setSettings((XmlFormatSettings) request.getSettings());
          break;
        default:
          throw new RuntimeException("Preview with custom settings not supported for format: "
              + descriptor.getFormat());
        }
      }
      if (request.getSettings() != null) {
        descriptor.setColumns(null);
      }
      try (Parser parser = parserFactory.getParser(descriptor, true);
          RecordIterator it = parser.parse()) {
        while (it.hasNext() && count++ < request.getLimit()) {
          result.add(it.next());
        }
        // if new settings are provided then schema has to be re-defined
        if (request.getSettings() != null) {
          List<ColumnInfo> columns = tryParseColumns(
              new CollectionDelegateParser(new CollectionDelegateDescriptor(result)));
          descriptor.setColumns(columns);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    log.info("Preview data retrieved for {}", stopwatch.stop());
    return new PreviewResponse(descriptor, result);
  }

  private MessageDigest getMessageDigest() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return descriptor id
   */
  public Long saveFile(UploadFileRequest request, FileItemStream item) throws IOException {
    String fileName = item.getName();
    String contentType = item.getContentType();
    DataFormat dataFormat = tryToGuessFileFormat(null, fileName, contentType, null);
    Long fileSize = request.getFileSize();
    StorageType storageType = storageSelectionStrategy.getStorageType(fileSize, dataFormat);
    log.info("Saving file [format={} size={} " + "to=" + storageType + "]", dataFormat, fileSize);

    DigestInputStream digestInputStream = new DigestInputStream(item.openStream(), getMessageDigest());
    BufferingInputStream bis = InputStreamUtils.buffering(digestInputStream);
    String path = storageStrategy.get(storageType).saveFile(bis);
    String checksum = new BigInteger(1, digestInputStream.getMessageDigest().digest()).toString();

    ConfirmUploadRequest confirmRequest = new ConfirmUploadRequest();
    confirmRequest.setContentType(item.getContentType());
    confirmRequest.setFileName(item.getName());
    confirmRequest.setFileSize(request.getFileSize());
    return registerFile(confirmRequest, dataFormat, checksum, path, storageType, new ByteArrayInputStream(bis.getBufferContents()));
  }

  public Long registerFile(ConfirmUploadRequest request, DataFormat dataFormat, String checksum, String path, StorageType storageType, InputStream eofBuffer) throws IOException {
    String fileName = request.getFileName();
    String extension = org.apache.commons.io.FilenameUtils.getExtension(fileName);
    String contentType = request.getContentType();
    Long fileSize = request.getFileSize();
    checkFileExisting(checksum);
    checkFileFormat(extension, dataFormat, storageType, path);
    checkFileSize(dataFormat, fileName, fileSize);
    String bufferPath = storageStrategy.getDefault().saveFile(eofBuffer);
    FileDescriptor fileDescriptor = FileDescriptor.builder()
        .size(fileSize)
        .originalFileName(fileName)
        .extension(extension)
        .contentType(contentType)
        .checksum(checksum)
        .bufferPath(bufferPath)
        .storage(storageType)
        .path(path)
        .build();
    fileDescriptor.setFormat(dataFormat);

    Descriptor descriptor = DescriptorFactory.getDescriptorFromAnotherOne(dataFormat, fileDescriptor);
    return uploadRepository.saveDescriptor(descriptor);
  }

  public void checkFileExisting(String checksum) throws ResourceAlreadyExists {
    Upload upload = uploadRepository.getUploadByChecksum(checksum);
    if (upload != null) {
      throw new ResourceAlreadyExists("File", upload.getName(), upload.getId());
    }
  }

  private void checkFileSize(DataFormat dataFormat, String fileName, Long fileSize) {
    if (!dataFormat.isFileSizeAcceptable(fileSize)) {
      throw new TooLargeFileException(fileName, fileSize, dataFormat);
    }
  }

  private void checkFileFormat(String extension, DataFormat dataFormat, StorageType storageType, String path) {
    if (dataFormat == DataFormat.UNDEFINED) {
      storageStrategy.get(storageType).removeFile(path);
      if (StringUtils.isBlank(extension)) {
        extension = "This";
      } else {
        extension = extension.toUpperCase();
      }
      throw ExecutionException.of("invalid_file", "Not acceptable",
          extension + " is not an acceptable format. Acceptable formats are csv, tsv, xml, and json.");
    }
  }

  private DataFormat tryToGuessFileFormat(FileStorage storage, String fileName, String contentType, String filePath) {
    // try to guess upload format by looking into extension and content type
    List<DataFormat> possibleFormats = Arrays.stream(DataFormat.values())
        .filter(dataFormat -> dataFormat.testExtension(fileName) || dataFormat.testContentType(contentType))
        .collect(Collectors.toList());

    // fallback to content test, if there is no matching items\no extension
    if (possibleFormats.isEmpty()) {
      possibleFormats = Arrays.stream(DataFormat.values())
          .filter(dateFormat -> dateFormat.testContents(storage, filePath))
          .collect(Collectors.toList());
    }

    if (possibleFormats.size() > 1) {
      log.warn("Can not explicitly define file format. " +
          "Possible file formats {} for file '{}'", possibleFormats, fileName);
    }
    if (possibleFormats.isEmpty()) {
      throw new RuntimeException("Unable to determine file format.");
    }
    return possibleFormats.get(0);
  }

  public List<Map<AbstractParsedColumn, Object>> retrieveRows(Parser parser) throws IOException {
    int maxRows = 5000;
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>(maxRows);

    try (RecordIterator it = parser.parse()) {
      int i = 0;
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (i++ < maxRows && it.hasNext()) {
        result.add(it.next());
      }
      log.info("Retrieving first {} rows took {}", maxRows, stopwatch.stop());
      return result;
    }
  }

  public List<ColumnInfo> columnsInfoFromRows(List<Map<AbstractParsedColumn, Object>> rows) throws IOException {
    int rangeConstrainedToLimit = Math.min(rows.size(), 1000);
    List<Map<AbstractParsedColumn, Object>> rowsForProcess = new ArrayList<>(rows.subList(0, rangeConstrainedToLimit));
    ColumnDetector columnDetector = new ColumnDetector();
    return columnDetector.processRows(rowsForProcess);
  }

  public List<ColumnInfo> tryParseColumns(Parser parser) throws IOException {
    int maxRows = 1000;
    ColumnDetector columnDetector = new ColumnDetector();
    try (RecordIterator it = parser.parse()) {
      int i = 0;
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (i++ < maxRows && it.hasNext()) {
        it.next();
        columnDetector.processCurrentRow(it);
      }
      log.info("Columns parsed in {}", stopwatch.stop());
      return columnDetector.getColumns();
    }
  }

  private void preprocessUpload(Upload upload, boolean onlySchema) {
    preprocessUpload(upload, onlySchema, null);
  }

  private void preprocessUpload(Upload upload, boolean onlySchema, Parser parser) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    // do pre-processing
    FileDescriptor descriptor = (FileDescriptor) upload.getDescriptor();
    try {
      boolean closeParser = false;
      if (parser == null) {
        closeParser = true;
        parser = parserFactory.getParser(descriptor);
      }
      if (!onlySchema) {
        switch (descriptor.getFormat()) {
        case CSV:
          CsvFileDescriptor csvFileDescriptor = (CsvFileDescriptor) descriptor;
          csvFileDescriptor.setSettings(((CSVParser) parser).tryGetCsvSettings());
          break;
        case XLS_SHEET:
        case XLSX_SHEET:
          XlsFileDescriptor xlsFileDescriptor = (XlsFileDescriptor) descriptor;
          xlsFileDescriptor.getSettings().setUseHeaders(true);
          xlsFileDescriptor.getSettings().setSkipFromBottom(0);

          break;
        case XML:
          try (InputStream stream = storageStrategy.get(descriptor).getFile(descriptor.getPath())) {
            XmlFileDescriptor xmlFileDescriptor = (XmlFileDescriptor) descriptor;
            Multiset<String> availableRowXPathsSet = XmlParser.getAvailableRowXPaths(stream);
            List<String> availableRowXPaths = new ArrayList<>(availableRowXPathsSet.elementSet());
            Collections.sort(availableRowXPaths);

            xmlFileDescriptor.setAvailableRowXPaths(availableRowXPaths);
            if (availableRowXPaths.size() > 0) {
              // set deeper path if file size is too big (probably first-level objects are too big)
              for (String xpath : availableRowXPaths) {
                int count = availableRowXPathsSet.count(xpath);
                xmlFileDescriptor.getSettings().setRowXPath(xpath);
                if (count > 1) {
                  break;
                }
              }
            } else {
              throw new RuntimeException("Can't determine rows in XML file");
            }
          }
          break;
        }
      }
      // define schema and row count
      long fileSize = storageStrategy.get(descriptor).getFileSize(descriptor.getPath());
      Pair<Long, Boolean> rowCount = parser.getRowsEstimateCount(fileSize);
      if (rowCount != null) {
        if (rowCount.getRight()) {
          descriptor.setRowsExactCount(rowCount.getLeft());
          descriptor.setRowsEstimatedCount(null);
        } else {
          descriptor.setRowsExactCount(null);
          descriptor.setRowsEstimatedCount(rowCount.getLeft());
        }
      } else {
        descriptor.setRowsExactCount(null);
        descriptor.setRowsEstimatedCount(null);
        descriptor.setValid(false);
      }
      descriptor.setColumns(null);
      List<Map<AbstractParsedColumn, Object>> rows = retrieveRows(parser);
      descriptor.setColumns(columnsInfoFromRows(rows));
      Map<AbstractParsedColumn, Object> exampleValues = retrieveExampleValues(rows);
      descriptor.getColumns().forEach(col ->
      col.setExampleValue(exampleValues.getOrDefault(ParsedColumnFactory.getByColumnInfo(col), "").toString()));

      if (closeParser) {
        parser.close();
      }
      upload.setPreviewDescriptor(filePreviewService.copy(descriptor, false, rows));
      descriptor.setValid(true);
      descriptor.setErrorCode(null);
      descriptor.setErrorString(null);
      log.info("Pre-processing upload took {}", stopwatch.stop());
    } catch (ExecutionException e) {
      log.error("Can't preprocess", e);
      ExecutionException.Error error = e.getFirstError();
      descriptor.setErrorCode(error.getCode());
      descriptor.setErrorString(error.getMessage());
    } catch (Exception e) {
      log.error("Can't preprocess", e);
      descriptor.setErrorCode(null);
      descriptor.setErrorString(e.getMessage());
    }

  }

  public File createFile(Long userId, Long descriptorId, Long parentId) throws IOException {
    FileDescriptor descriptor = uploadRepository.getFileDescriptor(descriptorId);
    descriptor.getOptions().put("LAST_BUFFERED_CONTENTS", IOUtils.toByteArray(storageStrategy.getDefault().getFile(descriptor.getBufferPath())));
    if (descriptor.getFormat() != null) {
      switch (descriptor.getFormat()) {
      case XLSB:
        throw new RuntimeException("Only xls and xlsx Excel files are supported.");
      }
    }

    if (descriptor.getFormat() == null) {
      File file = new File();
      file.setName(descriptor.getOriginalFileName());
      file.setUser(userRepository.getUser(userId));
      file.setUploaded(new Date());
      file.setDescriptor(descriptor);

      if (parentId != null) {
        Folder folder = (Folder) uploadRepository.getFile(parentId);
        file.setParent(folder);
      }

      uploadRepository.saveFile(file);
      return file;
    }

    Upload upload = new Upload();
    upload.setName(descriptor.getOriginalFileName());
    upload.setUser(userRepository.getUser(userId));
    upload.setChecksum(descriptor.getChecksum());
    Descriptor oldDescriptor = null;
    if (parentId != null) {
      AbstractFile abstractFile = uploadRepository.getFile(parentId, false, true, false);
      if (abstractFile instanceof Folder) {
        Folder folder = (Folder) abstractFile;
        upload.setParent(folder);
      } else if (abstractFile instanceof Upload
          && !((Upload) abstractFile).getDescriptor().isRemote()) {
        log.debug("Replacing file {} contents", parentId);
        upload = ((Upload) abstractFile);
        oldDescriptor = upload.getDescriptor();
      }
    }

    upload.setUploaded(new Date());
    upload.setDescriptor(descriptor);

    List<Upload> children = new ArrayList<>();


    Map<String, Upload> namedSections = new HashMap<>();
    if (upload.getSections() != null) {
      namedSections = upload.getSections().stream().collect(Collectors.toMap(AbstractFile::getName, u -> u));
    }

    if (!descriptor.getFormat().options().isComposite()) {
      preprocessUpload(upload, false);
    } else {
      if (DataFormat.XLS.equals(descriptor.getFormat())) {
        XlsFileDescriptor tmpDescriptor = new XlsFileDescriptor();
        tmpDescriptor.setStorage(descriptor.getStorage());
        tmpDescriptor.setPath(((FileDescriptor) upload.getDescriptor()).getPath());
        Pair<Integer, Integer> fileSetting = predictFileSetting(new XLSParser(storageStrategy.get(descriptor), tmpDescriptor));
        tmpDescriptor.getSettings().setStartOnRow(fileSetting.getLeft());
        tmpDescriptor.getSettings().setSkipAfterHeader(fileSetting.getRight());

        try (XLSParser parser = new XLSParser(storageStrategy.get(descriptor), tmpDescriptor)) {
          List<String> sheets = parser.getSheets();
          AtomicLong index = new AtomicLong(0);
          Upload finalUpload = upload;
          Map<String, Upload> finalNamedSections = namedSections;
          sheets.forEach((sheetName) -> {
            XlsFileDescriptor subDescriptor = new XlsFileDescriptor();
            subDescriptor.setPath(tmpDescriptor.getPath());
            subDescriptor.setSize(descriptor.getSize());
            subDescriptor.setSheetIndex(index.getAndIncrement());
            subDescriptor.setSheetName(sheetName);
            subDescriptor.setFormat(DataFormat.XLS_SHEET);
            subDescriptor.setStorage(descriptor.getStorage());
            subDescriptor.setSettings(tmpDescriptor.getSettings());

            parser.setCurrentSheet(sheetName);

            Upload sheet = new Upload();
            if (finalNamedSections.containsKey(sheetName)) {
              sheet = finalNamedSections.get(sheetName);
            }
            sheet.setName(sheetName);
            sheet.setDescriptor(subDescriptor);
            sheet.setUploaded(finalUpload.getUploaded());
            sheet.setParent(finalUpload);
            sheet.setUser(userRepository.getUser(userId));
            preprocessUpload(sheet, false, parser);
            children.add(sheet);
          });
        }
      } else if (DataFormat.XLSX.equals(descriptor.getFormat())) {
        XlsFileDescriptor tmpDescriptor = new XlsFileDescriptor();
        tmpDescriptor.setPath(((FileDescriptor) upload.getDescriptor()).getPath());
        tmpDescriptor.setStorage(descriptor.getStorage());
        if (descriptor.getSize() > ParserFactory.XLSX_THRESHOLD_SIZE) {
          FileStorage storage = storageStrategy.get(descriptor);
          Map<String, String> sheets;
          try (XlsxStreamingParser parser = new XlsxStreamingParser(storage, tmpDescriptor)) {
            sheets = parser.getSheets();
          }
          long index = 0;
          for (Map.Entry<String, String> sheet : sheets.entrySet()) {
            XlsFileDescriptor subDescriptor = new XlsFileDescriptor();
            subDescriptor.setPath(descriptor.getPath());
            subDescriptor.setSheetIndex(index++);
            subDescriptor.setSheetName(sheet.getKey());
            subDescriptor.setSize(descriptor.getSize());
            subDescriptor.setFormat(DataFormat.XLSX_SHEET);
            subDescriptor.setStorage(descriptor.getStorage());
            subDescriptor.setSize(descriptor.getSize());
            Upload sheetUpload = new Upload();
            if (namedSections.containsKey(sheet.getValue())) {
              sheetUpload = namedSections.get(sheet.getValue());
            }
            sheetUpload.setName(sheet.getValue());
            sheetUpload.setDescriptor(subDescriptor);
            sheetUpload.setUploaded(upload.getUploaded());
            sheetUpload.setParent(upload);
            sheetUpload.setUser(userRepository.getUser(userId));
            try (XlsxStreamingParser parser = new XlsxStreamingParser(storageStrategy.get(descriptor), subDescriptor)) {
              preprocessUpload(sheetUpload, false, parser);
            } catch (Exception e) {
              continue;
            }
            if (subDescriptor.isValid()) {
              children.add(sheetUpload);
            }
          }
        } else {
          //                    todo do it more general
          Pair<Integer, Integer> fileSetting = predictFileSetting(new XLSXParser(storageStrategy.get(descriptor), tmpDescriptor));
          tmpDescriptor.getSettings().setStartOnRow(fileSetting.getLeft());
          tmpDescriptor.getSettings().setSkipAfterHeader(fileSetting.getRight());

          try (XLSXParser parser = new XLSXParser(storageStrategy.get(descriptor), tmpDescriptor)) {
            List<String> sheets = parser.getSheets();
            long index = 0;
            for (String sheetName : sheets) {

              XlsFileDescriptor subDescriptor = new XlsFileDescriptor();
              subDescriptor.setPath(tmpDescriptor.getPath());
              subDescriptor.setSheetIndex(index++);
              subDescriptor.setSheetName(sheetName);
              subDescriptor.setSize(descriptor.getSize());
              subDescriptor.setFormat(DataFormat.XLSX_SHEET);
              subDescriptor.setStorage(descriptor.getStorage());
              subDescriptor.setSettings(tmpDescriptor.getSettings());

              parser.setCurrentSheet(sheetName);

              Upload sheet = new Upload();
              if (namedSections.containsKey(sheetName)) {
                sheet = namedSections.get(sheetName);
              }
              sheet.setName(sheetName);
              sheet.setDescriptor(subDescriptor);
              sheet.setUploaded(upload.getUploaded());
              sheet.setParent(upload);
              sheet.setUser(userRepository.getUser(userId));
              preprocessUpload(sheet, false, parser);
              children.add(sheet);
            }
          }
        }
      }
    }
    if (upload.getId() == null) {
      upload.setSections(children);
      uploadRepository.saveFile(upload);
    } else {
      for (Upload section : upload.getSections()) {
        if (!children.contains(section)) {
          try {
            uploadRepository.deleteUpload(section.getId());
          } catch (Exception e) {
            log.error("Can't delete old section", e);
          }
        } else {
          uploadRepository.updateFile(section);
        }
      }
      upload.setSections(children);
      uploadRepository.updateFile(upload);
      if (oldDescriptor != null) {
        storageStrategy.get(descriptor).removeFile(((FileDescriptor) oldDescriptor).getPath());
      }
    }

    return upload;
  }

  /**
   * left - headerRow, right - skipAfterHeaderCount
   */
  private Pair<Integer, Integer> predictFileSetting(Parser parser) throws IOException {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      Integer headerRow = parser.getMorePossibleHeaderIndex();
      Pair<Integer, Integer> results = Pair.of(headerRow, parser.getSkipRowsCountAfterHeader(headerRow));
      log.info("Predict file settings took {}", stopwatch.stop());
      return results;
    } finally {
      parser.close();
    }
  }

  private String objectToString(Object obj) {
    if (obj == null) {
      return null;
    }

    if (obj instanceof String) {
      return (String) obj;
    } else if (obj instanceof Date) {
      return DateUtils.formatDateISO((Date) obj);
    } else if (obj instanceof Integer || obj instanceof Long) {
      return String.format("%d", obj);
    } else if (obj instanceof Float || obj instanceof Double) {
      return Double.toString((Double) obj);
    }
    return obj.toString();
  }

  public Map<AbstractParsedColumn, Object> retrieveExampleValues(List<Map<AbstractParsedColumn, Object>> previewData) {
    Map<AbstractParsedColumn, Object> result = new HashMap<>();
    if(previewData.size() == 0) {
      return result;
    }

    for (Map<AbstractParsedColumn, Object> row : previewData) {
      Set<AbstractParsedColumn> filledValues = result.keySet();
      Set<AbstractParsedColumn> remainingValues = row.keySet().stream().filter(complement(filledValues::contains)).collect(Collectors.toSet());
      remainingValues.forEach(remainingValueKey -> {
        Object val = row.get(remainingValueKey);
        if (val != null) {
          result.put(remainingValueKey, objectToString(val));
        }
      });
    }
    return result;
  }

  public Map<AbstractParsedColumn, Object> retrieveExampleValues(String filePath, Descriptor descriptor) {
    Integer oldLimit = descriptor.getLimit();
    descriptor.setLimit(1000);
    List<Map<AbstractParsedColumn, Object>> previewData = previewData(defaultPreviewRequestForColumns(filePath), descriptor).getData();
    descriptor.setLimit(oldLimit);
    return retrieveExampleValues(previewData);
  }

  private PreviewRequest defaultPreviewRequestForColumns(String path) {
    PreviewRequest previewRequest = new PreviewRequest();
    previewRequest.setLimit(1000);
    previewRequest.setPath(path);
    return previewRequest;
  }

  public void removeFile(Long fileId) {
    removeFiles(Collections.singletonList(fileId));
  }

  public void removeFiles(List<Long> fileIds) {
    List<AbstractFile> files = uploadRepository.getFiles(fileIds);
    for (AbstractFile file : files) {
      if (file instanceof Upload) {
        Upload upload = (Upload) file;
        if (upload.getDescriptor() instanceof FileDescriptor) {
          if (upload.getParent() == null
              || (((upload.getParent() instanceof Upload))
                  && ((Upload) uploadRepository.getFile(upload.getParentId(), false, true, false))
                  .getSections()
                  .size() == 1)) {
            try {
              FileDescriptor fileDescriptor = (FileDescriptor) upload.getDescriptor();
              storageStrategy.get(fileDescriptor).removeFile(fileDescriptor.getPath());
            } catch (Exception e) {
              log.error("Can't delete upload", e);
            }
          }
        }
      }
      if (file.getId() != null) {
        if (file instanceof Datadoc) {
          tableService.removeDatadoc(file.getId());
          shareService.cleanUpSharing(file.getId());
        } else {
          List<AbstractFile> children = uploadRepository.getFiles(file.getUser().getId(), file.getId());
          if (file instanceof Folder) {
            removeFiles(children.stream().map(f -> f.getId()).collect(Collectors.toList()));
          } else if (file instanceof Upload) {
            for (AbstractFile child : children) {
              uploadRepository.deleteUpload(child.getId());
            }
          }
          uploadRepository.deleteUpload(file.getId());
          log.debug("Removed file {}[{}]", file.getName(), file.getId());
        }

      }
    }
  }

  @Scheduled(cron = "0 0 */2 * * ?")
  public void checkDisconnectedLinks() {
    uploadRepository.getDbUploads().forEach(upload -> {
      DbDescriptor descriptor = (DbDescriptor) upload.getDescriptor();
      boolean isValid = descriptor.isValid();
      try {
        checkConnectivity(descriptor);
        descriptor.setValid(true);
        descriptor.setErrorString(null);
        descriptor.setErrorCode(null);
      } catch (ExecutionException e) {
        if (isValid) {
          mailService.send(upload.getUser().getEmail(), new DisconnectedSourceEmail(upload, null, false));
        }
        descriptor.setErrorCode(e.getFirstError().getCode());
        descriptor.setErrorString(e.getMessage());
        descriptor.setValid(false);
      }
      uploadRepository.updateDescriptor(descriptor);
    });
  }

  private void checkConnectivity(DbDescriptor descriptor) {
    try(Connection connection = dbConnectionProvider.getConnection(descriptor.getParams())) {
    } catch (SQLException e) {
      throw ExceptionWrapper.wrap(e);
    }
  }

  @Scheduled(cron = "0 0 0 * * ?")
  public void checkRemoteUploads() {
    List<RemoteLinkDescriptor> remoteLinkDescriptors = uploadRepository.getRemoteLinkDescriptors();

    long start = System.currentTimeMillis();
    Multimap<ConnectionParams, RemoteLinkDescriptor> groupedDescriptors =
        Multimaps.index(remoteLinkDescriptors, (d) -> d.getParams());

    for (ConnectionParams params : groupedDescriptors.keySet()) {

      Collection<RemoteLinkDescriptor> descriptors = groupedDescriptors.get(params);
      try (Connection conn = dbConnectionProvider.getConnection(params)) {

        List<RemoteLinkDescriptor> compositeDescriptors =
            descriptors.stream().filter(d -> !d.isSection()).collect(Collectors.toList());
        List<RemoteLinkDescriptor> sectionDescriptors =
            descriptors.stream().filter(d -> d.isSection()).collect(Collectors.toList());

        compositeDescriptors.forEach(d -> {
          d.setValid(true);
          d.setLastConnected(new Date());
          d.setErrorCode(null);
          d.setErrorString(null);
        });

        List<TableInfo> tables = dbService.getTables(params, conn, false);
        Set<String> tableNames = tables.stream().map(ti -> ti.getName()).collect(Collectors.toSet());

        sectionDescriptors.forEach(d -> {
          if (d instanceof DbTableDescriptor) {
            DbTableDescriptor tableDescriptor = (DbTableDescriptor) d;
            String tableName = tableDescriptor.getTableName();
            if (tableNames.contains(tableName)) {
              tableDescriptor.setValid(true);
              tableDescriptor.setLastConnected(new Date());
              tableDescriptor.setErrorCode(null);
              tableDescriptor.setErrorString(null);
            } else {
              tableDescriptor.setValid(false);
              tableDescriptor.setErrorCode("db_table_not_found");
              tableDescriptor.setErrorString("Table not found");
            }
          }
        });

      } catch (ExecutionException e) {
        descriptors.forEach(descriptor -> {
          descriptor.setValid(false);
          descriptor.setErrorCode(e.getFirstError().getCode());
          descriptor.setErrorString(e.getFirstError().getMessage());
        });
      } catch (Exception e) {
        descriptors.forEach(descriptor -> {
          descriptor.setValid(false);
          descriptor.setErrorString(ExceptionUtils.getRootCauseMessage(e));
        });
      } finally {
        uploadRepository.updateDescriptors(descriptors);
      }
    }
    log.info("Refreshed links to remote data sources in " + (System.currentTimeMillis() - start));
  }

  private List<Long> getNestedFiles(Long userId, List<Long> fileIds) {
    List<Long> result = new ArrayList<>();
    result.addAll(fileIds);
    for (Long fileId : fileIds) {
      List<Long> children = uploadRepository.getFiles(userId, fileId).stream()
          .filter(f -> f instanceof Upload)
          .map(f -> f.getId())
          .collect(Collectors.toList());
      result.addAll(getNestedFiles(userId, children));
    }
    return result;
  }

  public Map<Long, Collection<Long>> getFilesAttachedToDatadocs(Long userId, List<Long> fileIds) {
    List<Long> allFiles = getNestedFiles(userId, fileIds);
    return uploadRepository.getDatadocsForRemoteUpload(allFiles);
  }

  public AbstractFile annotate(Long id, String annotation) {
    AbstractFile file = uploadRepository.getFile(id);
    if (file == null) {
      throw new RuntimeException("File not found");
    }
    file.setAnnotation(annotation);
    uploadRepository.updateFile(file);
    return file;
  }

  public void closeStickyNote(Long fileId, StickyNoteType stickyNoteType) {
    AbstractFile file = uploadRepository.getFile(fileId);
    if (file == null) {
      throw new RuntimeException("File not found");
    }
    file.getClosedNotes().add(stickyNoteType);
    uploadRepository.updateFile(file);
  }
}
