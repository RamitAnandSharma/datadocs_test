package com.dataparse.server.service.es.index;

import com.dataparse.server.auth.*;
import com.dataparse.server.service.docs.*;
import com.dataparse.server.service.es.*;
import com.dataparse.server.service.flow.ErrorValue;
import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.util.*;
import com.dataparse.server.util.SystemUtils;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.Resources;
import com.google.gson.*;
import io.searchbox.action.*;
import io.searchbox.client.*;
import io.searchbox.core.*;
import io.searchbox.indices.*;
import io.searchbox.indices.mapping.*;
import lombok.extern.slf4j.*;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class IndexService {

  private final static String ES_INDEX_THREADS = "ES_INDEX_THREADS";

  @Autowired
  private ElasticClient elasticClient;

  @Autowired
  private TableRepository tableRepository;

  @Autowired
  private TaskManagementService taskManagementService;

  private ExecutorService taskExecutor;
  private Semaphore taskExecutorSemaphore;

  @PostConstruct
  protected void init() {
    int executorSize = SystemUtils.getProperty(ES_INDEX_THREADS, Runtime.getRuntime().availableProcessors());
    log.info("Initialized with executor size: " + executorSize);
    taskExecutor = Executors.newFixedThreadPool(executorSize);
    taskExecutorSemaphore = new Semaphore(executorSize + 10);
  }

  @PreDestroy
  protected void destroy() {
    taskExecutor.shutdownNow();
  }

  protected List<ErrorValue> getErrors(JestResult result, List<ColumnInfo> columns) {
    Map<String, List<ColumnInfo>> columnsByAlias = columns.stream().collect(Collectors.groupingBy(ColumnInfo::getAlias));

    List<ErrorValue> errors = new ArrayList<>();
    result.getJsonObject().get("items").getAsJsonArray().forEach(item -> {
      JsonElement op = item.getAsJsonObject().get("update");
      if (op == null) {
        op = item.getAsJsonObject().get("index");
      }
      if (op == null) {
        op = item.getAsJsonObject().get("create");
      }
      if (op == null) {
        op = item.getAsJsonObject().get("delete");
      }
      if (op != null) {
        JsonElement error = op.getAsJsonObject().get("error");
        if (error != null) {
          errors.add(new ErrorValue(getErrorMessage(columnsByAlias, error)));
        }
      }
    });
    return errors;
  }

  private String getErrorMessage(Map<String, List<ColumnInfo>> columns, JsonElement error) {
    String errorMessage = "value %s in %s could not be converted to a %s";
    JsonElement causedBy = error.getAsJsonObject().get("caused_by");
    JsonElement errorType = error.getAsJsonObject().get("type");
    JsonElement reason = error.getAsJsonObject().get("reason");
    if(reason != null && errorType != null && errorType.getAsString().equals("mapper_parsing_exception")) {
      if (causedBy != null) {
        JsonElement causedByReason = causedBy.getAsJsonObject().get("reason");
        String value = tryParseValue(causedByReason.toString());
        String alias = tryParseColumn(reason.toString());
        Optional<ColumnInfo> first = columns.getOrDefault(alias, Arrays.asList()).stream().findFirst();
        String columnName = first.isPresent() ? first.get().getName(): alias;
        String columnType = first.isPresent() ? first.get().getType().getDataType().getName(): "Undefined";
        return String.format(errorMessage, value, columnName, columnType);
      }
    } else {
      return error.toString();
    }
    return "";
  }

  private String tryParseValue(String str) {
    return FunctionUtils.invokeSilentWithDefault(() -> str.split(":")[1], "");
  }

  private String tryParseColumn(String str) {
    return FunctionUtils.invokeSilentWithDefault(() -> {
      String splited = str.split("\\[")[1];
      return splited.substring(0, splited.length() - 2);
    }, "");
  }

  String createIndex(Descriptor descriptor) {
    String externalId = UUID.randomUUID().toString();
    try {
      URL settingsURL = Resources.getResource("elastic/index-settings.json");
      String settings = Resources.toString(settingsURL, Charsets.UTF_8);
      elasticClient.getClient().execute(new CreateIndex.Builder(externalId).settings(settings).build());
      String mappingJson;
      if (descriptor.isUseTuplewiseTransform()) {
        URL mappingURL = Resources.getResource("elastic/index-mapping.json");
        mappingJson = Resources.toString(mappingURL, Charsets.UTF_8);
      } else {
        Map<String, Object> props = new HashMap<>();
        for(ColumnInfo col: descriptor.getColumns()){
          String typeString;

          final TypeDescriptor type;
          if(col.getSettings().getType() == null){
            type = col.getType();
          } else {
            type = col.getSettings().getType();
          }
          final SearchType searchType;
          if (col.getSettings().getSearchType() == null) {
            if(type.getDataType().equals(DataType.STRING)) {
              searchType = SearchType.EDGE;
            } else {
              searchType = SearchType.NONE;
            }
          } else {
            searchType = col.getSettings().getSearchType();
          }

          switch (type.getDataType()){
          case TIME:
            typeString = "long";
            break;
          case DECIMAL:
            typeString = "double";
            break;
          case DATE:
            typeString = "date";
            break;
          case STRING:
            typeString = "keyword";
            break;
          default:
            throw new RuntimeException("Unknown type: " + col.getType().getDataType());
          }
          Map<String, Object> field = new HashMap<>();
          props.put(col.getAlias(), field);
          field.put("type", typeString);
          switch (type.getDataType()){
          case STRING:
            field.put("index", "not_analyzed");
            Map<String, Object> variations = new HashMap<>();
            variations.put("default", ImmutableMap.of("type", "text", "analyzer", "standard"));
            switch (searchType){
            case NONE:
              break;
            case EDGE:
              variations.put("e_ngram", ImmutableMap.of("type", "text", "analyzer", "index_edge_ngram_analyzer"));
              break;
            case FULL:
              variations.put("ngram", ImmutableMap.of("type", "text", "analyzer", "index_ngram_analyzer"));
            }
            if(!col.getSettings().isDisableFacets()){
              variations.put("sort", ImmutableMap.of("type", "string", "analyzer", "sortable_analyzer", "fielddata", true));
            }
            field.put("fields", variations);
            break;
          case DATE:
            field.put("format", "epoch_millis");
            break;
          }
        }
        mappingJson = JsonUtils.writeValue(ImmutableMap.of("_all", ImmutableMap.of("enabled", false), "properties", props));
      }
      elasticClient.getClient().execute(new PutMapping.Builder(externalId, "row", mappingJson).build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return externalId;
  }

  public void removeIndex(String externalId) {
    try {
      elasticClient.getClient().execute(new DeleteIndex.Builder(externalId).build());
    } catch (Exception e) {
      log.error("Can't delete index", e);
    }
  }

  void refreshIndex(String externalId) {
    try {
      elasticClient.getClient().execute(new Refresh.Builder().addIndex(externalId).build());
    } catch (IOException e) {
      log.error("Can't refresh index", e);
    }
  }

  protected JestResult execute(Action action) {
    JestResult result = null;
    try {
      result = elasticClient.getClient().execute(action);
      if (!result.isSucceeded()) {
        throw new RuntimeException(result.getErrorMessage());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public IndexResult delete(Auth auth, Long bookmarkId, List<String> ids) {
    TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
    if (bookmark == null) {
      throw new RuntimeException("Table not found");
    }
    DeleteRequest task = new DeleteRequest(bookmarkId, ids);
    return (IndexResult) taskManagementService.executeSync(auth, task).getResult();
  }

  public IndexResult index(Auth auth, Long bookmarkId, Boolean strictCheck, boolean preserveHistory, IngestErrorMode ingestErrorMode, Descriptor descriptor) {
    TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
    if (bookmark == null) {
      throw new RuntimeException("Table not found");
    }

    IndexRequest task = new IndexRequest(bookmarkId, strictCheck, preserveHistory, ingestErrorMode, descriptor);
    return (IndexResult) taskManagementService.executeSync(auth, task).getResult();
  }

  public String indexAsync(Auth auth, Long bookmarkId, Boolean strictCheck, boolean preserveHistory, IngestErrorMode ingestErrorMode, Descriptor descriptor) {
    TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
    if (bookmark == null) {
      throw new RuntimeException("Bookmark not found");
    }

    IndexRequest task = new IndexRequest(bookmarkId, strictCheck, preserveHistory, ingestErrorMode, descriptor);
    task.setParentTask(MDC.get("request"));
    return taskManagementService.execute(auth, task);
  }

  public IndexResult executeBulk(String externalId, List<BulkableAction> actions, List<ColumnInfo> columns) {
    JestResult result = null;
    boolean indexed = false;

    Bulk bulkRequest = new Bulk.Builder()
        .defaultIndex(externalId)
        .defaultType("row")
        .addAction(actions)
        .build();

    long start = System.currentTimeMillis();
    while (!indexed) {
      try {
        result = elasticClient.getClient().execute(bulkRequest);
        indexed = true;
      } catch (IOException e) {
        log.error("Can't execute bulk actions", e);
        try {
          Thread.sleep(1000);
          log.error("Can't perform bulk request, will retry", e.getMessage());
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e1);
        }
      }
    }
    IndexResult indexResult = new IndexResult(externalId,
        System.currentTimeMillis() - start,
        0, 0, 0.,
        result.isSucceeded());
    indexResult.setTotal(actions.size());
    List<ErrorValue> errors = getErrors(result, columns);
    indexResult.addProcessionErrors(errors);
    return indexResult;
  }

  public Future<IndexResult>  executeBulkAsync(String externalId, List<BulkableAction> actions, List<ColumnInfo> columns, FunctionUtils.SimpleFunction onAfterFinish) {
    long waitTime;
    try {
      long start = System.currentTimeMillis();
      taskExecutorSemaphore.acquire();
      waitTime = System.currentTimeMillis() - start;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return taskExecutor.submit(() -> {
      try{
        IndexResult result = executeBulk(externalId, actions, columns);
        result.setWaitTime(waitTime);
        onAfterFinish.invoke();
        return result;
      } finally {
        taskExecutorSemaphore.release();
      }
    });
  }

  private Object getIn(Object o, List<String> path) {
    if (path.isEmpty()) {
      return o;
    } else if (o instanceof Map) {
      return getIn(((Map) o).get(path.get(0)), path.subList(1, path.size()));
    }
    return null;
  }

  public Map getMappings(Long bookmarkId) {
    TableBookmark bookmark = tableRepository.getTableBookmark(bookmarkId);
    try {
      JestResult result = elasticClient.getClient().execute(new GetMapping.Builder().addIndex(bookmark.getTableSchema().getExternalId()).build());
      return (Map) getIn(result.getJsonMap(), Arrays.asList(bookmark.getTableSchema().getExternalId(), "mappings", "row", "properties"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Long getStorageSpace(String indexName){
    try {
      JestResult result = elasticClient.getClient().execute(new Stats.Builder().addIndex(indexName).build());
      JsonObject totals = result.getJsonObject().getAsJsonObject("_all").getAsJsonObject("total");
      return totals.getAsJsonObject("store").get("size_in_bytes").getAsLong();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void retrieveStorageStatistics(String indexName, BookmarkStatistics statistics){
    try {
      JestResult result = elasticClient.getClient().execute(new Stats.Builder().addIndex(indexName).build());
      JsonObject totals = result.getJsonObject().getAsJsonObject("_all").getAsJsonObject("total");
      statistics.setRows(totals.getAsJsonObject("docs").get("count").getAsLong());
      statistics.setStorageSpace(totals.getAsJsonObject("store").get("size_in_bytes").getAsLong());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
