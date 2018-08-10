package com.dataparse.server.controllers;

import com.dataparse.server.service.embed.*;
import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.google.common.collect.*;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/embed")
@Api(value = "Embed", description = "View embed")
public class EmbedController extends ApiController {

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @ApiOperation(value = "Get embed settings")
    @RequestMapping(method = RequestMethod.GET, value = "/{uuid}")
    public Map<String, Object> getEmbedSettings(@PathVariable String uuid) {
        EmbedSettings embedSettings = tableRepository.getTableBookmarkEmbedSettings(uuid);
        TableBookmark tableBookmark = embedSettings.getTableBookmark();
        BookmarkState embedState = bookmarkStateStorage.get(tableBookmark.getBookmarkStateId(), true).getState();
        return ImmutableMap.of(
                "datadocId", tableBookmark.getDatadoc().getId(),
                "bookmarkId", tableBookmark.getId(),
                "tableId", tableBookmark.getTableSchema().getId(),
                "settings", embedSettings,
                "state", embedState);
    }

}
