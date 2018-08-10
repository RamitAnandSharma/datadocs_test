package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.config.*;
import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.es.ElasticClient;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.storage.*;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadRepository;
import com.google.api.client.util.*;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;
import io.searchbox.indices.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/api/stats")
@ApiIgnore
@Api(value = "Statistics", description = "Statistic operations")
public class StatsController extends ApiController {

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    private IStorageStrategy storageStrategy;

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private TableRepository tableRepository;

    // todo use cache
    @ApiOperation(value = "Return statistics", notes = "Return of statistic of usage: size of storage, number of indexes, total size of indexes")
    @RequestMapping(method = RequestMethod.GET)
    public Map getStats() {
        JsonObject total;
        long docs = 0, indexSize = 0;
        Set<String> indexExternalIds = tableRepository.getUserSchemas(Auth.get().getUserId(), EngineType.ES);
        try {
            JestResult result = elasticClient.getClient().execute(new Stats.Builder().build());
            for (Map.Entry<String, JsonElement> entry : result
                    .getJsonObject()
                    .get("indices")
                    .getAsJsonObject()
                    .entrySet()) {
                total = entry.getValue().getAsJsonObject().get("total").getAsJsonObject();
                if (indexExternalIds.contains(entry.getKey()) && !total.toString().equals("{}")) {
                    docs += total.getAsJsonObject("docs").get("count").getAsLong();
                    indexSize += total.getAsJsonObject("store").get("size_in_bytes").getAsLong();
                }
            }
        } catch (UnsupportedOperationException e) {
            // do nothing (found.elastic.io returns 404 on all state requests when no indices available)
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<Upload> uploads = uploadRepository.getUserUploads(Auth.get().getUserId());
        Long storageSize = uploads
                .stream()
                .mapToLong(u -> {
                    FileDescriptor descriptor = (FileDescriptor) u.getDescriptor();
                    return storageStrategy.get(descriptor).getFileSize(descriptor.getPath());
                })
                .sum();

        return ImmutableMap.of(
                "docs", docs,
                "indexSize", indexSize,
                "fileSize", storageSize
                              );
    }

    @RequestMapping(method = RequestMethod.POST, value = "/cleanup")
    public Map cleanOldIndices() {
        JsonObject total;
        long docs = 0, indexSize = 0, count = 0;
        Set<String> indexExternalIds = tableRepository.getUserSchemas(Auth.get().getUserId(), EngineType.ES);
        try {
            JestResult result = elasticClient.getClient().execute(new Stats.Builder().build());
            for (Map.Entry<String, JsonElement> entry : result
                    .getJsonObject()
                    .get("indices")
                    .getAsJsonObject()
                    .entrySet()) {
                total = entry.getValue().getAsJsonObject().get("total").getAsJsonObject();
                if (!indexExternalIds.contains(entry.getKey())) {
                    count++;
                    docs += total.getAsJsonObject("docs").get("count").getAsLong();
                    indexSize += total.getAsJsonObject("store").get("size_in_bytes").getAsLong();
                    elasticClient.getClient().execute(new DeleteIndex.Builder(entry.getKey()).build());
                }
            }
        } catch (UnsupportedOperationException e) {
            // do nothing (found.elastic.io returns 404 on all state requests when no indices available)
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ImmutableMap.of(
                "status", "Cleaned " + count + " indexes (" + docs + " docs of total size " + indexSize + " bytes)");
    }

}
