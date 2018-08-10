package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.file.UpdateSchemaFromBookmarkRequest;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.controllers.exception.ResourceNotFoundException;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.embed.EmbedSettings;
import com.dataparse.server.service.engine.EngineType;
import com.dataparse.server.service.es.index.IndexService;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableSchema;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.security.DatadocActionAccessibility;
import com.dataparse.server.service.security.DatadocSecurityService;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.upload.refresh.RefreshType;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkStateHistoryWrapper;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkStateRepository;
import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/docs/bookmarks")
@Api(value = "Table Bookmarks", description = "Operations with indexed tables' bookmarks")
public class BookmarksController {

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private TableService tableService;

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @Autowired
    private BookmarkStateRepository bookmarkStateRepository;

    @Autowired
    private DatadocSecurityService datadocSecurityService;

    @Autowired
    private IndexService indexService;


    private class UpdateRefreshSettingsRequestValidator implements Validator {
        @Override
        public boolean supports(Class<?> clazz) {
            return UpdateRefreshSettingsRequest.class.equals(clazz);
        }

        @Override
        public void validate(Object target, Errors errors) {
            UpdateRefreshSettingsRequest request = (UpdateRefreshSettingsRequest) target;
            if (StringUtils.isBlank(request.getSettings().getCronExpression())) {
                if (!request.getSettings().getType().equals(RefreshType.NONE)) {
                    errors.reject("Invalid CRON expression");
                }
            } else {
                try {
                    new CronTrigger(request.getSettings().getCronExpression(), request.getSettings().defineTimeZone());
                } catch (Exception e) {
                    errors.reject("Invalid CRON expression: " + request.getSettings().getCronExpression());
                }
            }
        }
    }

    @InitBinder("updateRefreshSettingsRequest")
    protected void initBinder(WebDataBinder binder) {
        binder.addValidators(new UpdateRefreshSettingsRequestValidator());
    }

    @ApiOperation(value = "Return all table bookmarks")
    @RequestMapping(value = "/all", method = RequestMethod.GET)
    public List<TableBookmark> getAllBookmarks(@RequestParam Long datadocId, @RequestParam(required = false) Long tabId,
                                               @RequestParam(required = false) UUID stateId, @RequestParam boolean fetchState) {
        Datadoc datadoc = uploadRepository.getDatadocById(datadocId);
        datadocSecurityService.checkDatadocAccess(datadoc, DatadocActionAccessibility.GET_BOOKMARK);
        tableRepository.updateViewed(datadocId);
        List<TableBookmark> result;
        if(tabId == null) {
            result = tableRepository.getTableBookmarks(datadocId, fetchState);
        } else {
            result = tableRepository.getTableBookmarksWithSpecificState(datadocId, tabId, stateId, fetchState);
        }
        if(result == null || result.size() == 0) {
            throw new ResourceNotFoundException(datadocId, datadoc.getName(), datadoc.isDeleted());
        }
        return result;
    }

    @ApiOperation(value = "Return table bookmark state")
    @RequestMapping(value = "/state", method = RequestMethod.GET)
    public TableBookmark getTableBookmark(@RequestParam Long tabId, @RequestParam(required = false) UUID stateId,
                                          @RequestParam(required = false) Long userId) {
        TableBookmark tableBookmark = userId == null
                ? tableRepository.getTableBookmark(tabId, stateId)
                : tableRepository.getTableBookmarkForUser(tabId, userId);
        tableBookmark.setCurrentState(stateId);
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.GET_BOOKMARK);
        return tableBookmark;
    }

    @ApiOperation(value = "Delete table bookmark state")
    @RequestMapping(value = "/state/{tabId}/{stateId}", method = RequestMethod.DELETE)
    public void deleteBookmarkState(@PathVariable Long tabId, @PathVariable UUID stateId){
        tableRepository.removeBookmarkState(tabId, stateId);
    }

    @ApiOperation(value = "Return table bookmark specific state")
    @RequestMapping(value = "/change_bookmark_state", method = RequestMethod.POST)
    public BookmarkStateHistoryWrapper getBookmarkState(@RequestBody GetBookmarkRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId(), request.getStateId());
        boolean isDefaultState = tableBookmark.getDefaultState().equals(request.getStateId());
        tableBookmark.setCurrentState(request.getStateId());

        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.GET_BOOKMARK);
        BookmarkStateHistoryWrapper state = bookmarkStateStorage.get(request.getBookmarkStateId(), true, isDefaultState);
        return state;
    }

    @ApiOperation(value = "Save current bookmark state")
    @RequestMapping(value = "/save_state", method = RequestMethod.POST)
    public TableBookmark saveTableBookmarkState(@RequestBody SaveBookmarkStateRequest request){
        return tableService.saveTableBookmarkView(request.getTabId(), request.getStateId(), request.getName());
    }

    @RequestMapping(value = "/preset_default", method = RequestMethod.POST)
    public TableBookmark presetTableBookmarkDefaultState(@RequestBody PresetBookmarkStateRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId());
        boolean isDefaultState = tableBookmark.getDefaultState().equals(request.getStateId());
        if(!isDefaultState) {
            BookmarkStateId bookmarkStateId = new BookmarkStateId(request.getTabId(), request.getStateId(), Auth.get().getUserId());
            BookmarkState state = request.isToCleanState()
                    ? bookmarkStateStorage.get(new BookmarkStateId(tableBookmark.getId(), tableBookmark.getDefaultState(), Auth.get().getUserId()), true, true).getState()
                    : bookmarkStateStorage.get(bookmarkStateId, false, true).getState();
            bookmarkStateStorage.setDefaultStateFromAnotherOne(state, tableBookmark.getDefaultState());
        }
        return tableBookmark;
    }

    @ApiOperation(value = "Return table bookmark state")
    @RequestMapping(value = "/{id}/state", method = RequestMethod.GET)
    public BookmarkStateHistoryWrapper getTableBookmarkState(@PathVariable GetBookmarkRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId());
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.GET_BOOKMARK);
        boolean isDefaultState = tableBookmark.getDefaultState().equals(request.getStateId());
        return bookmarkStateStorage.get(request.getBookmarkStateId(), true, isDefaultState);
    }

    @ApiOperation(value = "Return table bookmark state before last change")
    @RequestMapping(value = "/{id}/undo", method = RequestMethod.POST)
    public BookmarkStateHistoryWrapper undo(@PathVariable GetBookmarkRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId());
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.UNDO_REDO_BOOKMARK);
        return bookmarkStateStorage.undo(request.getBookmarkStateId());
    }

    @ApiOperation(value = "Return table bookmark state after next change")
    @RequestMapping(value = "/{id}/redo", method = RequestMethod.POST)
    public BookmarkStateHistoryWrapper redo(@PathVariable GetBookmarkRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId());
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.UNDO_REDO_BOOKMARK);
        return bookmarkStateStorage.redo(request.getBookmarkStateId());
    }

    @ApiOperation(value = "Create table bookmark")
    @RequestMapping(method = RequestMethod.POST)
    public TableBookmark createTableBookmark(@RequestBody CreateTableBookmarkRequest request) {
        datadocSecurityService.checkDatadocAccess(request.getDatadocId(), DatadocActionAccessibility.CREATE_BOOKMARK);
        return tableService.createTableBookmark(request.getDatadocId(), request.getTabId(), request.getStateId());
    }

    @ApiOperation(value = "Update table bookmark")
    @RequestMapping(method = RequestMethod.PUT, value = "/{tableBookmarkId}")
    public TableBookmark updateTableBookmark(@PathVariable Long tableBookmarkId, @RequestBody UpdateTableBookmarkRequest request) {
        TableBookmark tableBookmark = tableRepository.getTableBookmark(tableBookmarkId);
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.UPDATE_BOOKMARK);
        return tableRepository.updateTableBookmark(tableBookmarkId, request);
    }

    @ApiOperation(value = "Update embed settings")
    @RequestMapping(method = RequestMethod.PUT, value = "/{tableBookmarkId}/embed")
    public EmbedSettings updateTableBookmarkEmbedSettings(@PathVariable Long tableBookmarkId, @RequestBody EmbedSettings settings) {
        return tableRepository.updateTableBookmarkEmbedSettings(tableBookmarkId, settings);
    }

    @ApiOperation(value = "Move table bookmark")
    @RequestMapping(value = "/move", method = RequestMethod.POST)
    public void moveBookmark(@RequestBody MoveTableBookmarkRequest request){
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTableBookmarkId());
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.MOVE_BOOKMARK);
        tableRepository.moveTableBookmark(request.getTableBookmarkId(), request.getToPosition());
    }

    @ApiOperation(value = "Delete table bookmark")
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteTableBookmark(@PathVariable Long id) {
        TableBookmark tableBookmark = tableRepository.getTableBookmark(id);
        datadocSecurityService.checkDatadocAccess(tableBookmark, DatadocActionAccessibility.DELETE_BOOKMARK);
        tableService.removeTableBookmark(id);
    }

    @ApiOperation(value = "Mapping for tables", notes = "Return mapping for table")
    @RequestMapping(value = "/{id}/mapping", method = RequestMethod.GET)
    public Map getIndexMapping(@PathVariable Long id) {
        TableBookmark bookmark = tableRepository.getTableBookmark(id);
        datadocSecurityService.checkDatadocAccess(bookmark, DatadocActionAccessibility.GET_BOOKMARK_MAPPING);

        TableSchema schema = bookmark.getTableSchema();
        if(!schema.getEngineType().equals(EngineType.ES)){
            throw new RuntimeException("ElasticSearch is not set as an engine");
        }
        return indexService.getMappings(schema.getId());
    }

// this mutation is unacceptable, but without it, we need to reimplement descriptors
    @PostMapping(value = "/preset_settings_from_bookmark")
    public Descriptor retrieveBookmarkDescriptor(@RequestBody UpdateSchemaFromBookmarkRequest request) {
        TableBookmark tableBookmark = tableRepository.getTableBookmark(request.getTabId());
        AbstractFile file = uploadRepository.getFile(request.getSourceId());
        Descriptor descriptor = tableBookmark.getTableSchema().getDescriptor();
        if(file instanceof Upload && descriptor != null) {
            Upload upload = (Upload) file;
            Descriptor uploadDescriptor = upload.getDescriptor();
            if(uploadDescriptor instanceof CsvFileDescriptor) {
                ((CsvFileDescriptor) uploadDescriptor).setSettings(((CsvFileDescriptor) descriptor).getSettings());
            } else if(uploadDescriptor instanceof XlsFileDescriptor) {
                ((XlsFileDescriptor) uploadDescriptor).setSettings(((XlsFileDescriptor) descriptor).getSettings());
            } else if(uploadDescriptor instanceof XmlFileDescriptor) {
                ((XmlFileDescriptor) uploadDescriptor).setSettings(((XmlFileDescriptor) descriptor).getSettings());
            }
            uploadRepository.updateDescriptors(Lists.newArrayList(uploadDescriptor));
            return uploadDescriptor;
        }
        return null;
    }

    @ApiOperation("Update refresh settings")
    @RequestMapping(value = "/{id}/update_refresh_settings", method = RequestMethod.POST)
    public RefreshSettings updateRefreshSettings(@PathVariable Long id, @Valid @RequestBody UpdateRefreshSettingsRequest updateRefreshSettingsRequest, Errors errors) {
        if (errors.hasErrors()) {
            throw new com.dataparse.server.controllers.exception.ValidationException(errors.getAllErrors());
        }
        TableBookmark bookmark = tableRepository.getTableBookmark(id);
        datadocSecurityService.checkDatadocAccess(bookmark, DatadocActionAccessibility.UPDATE_SETTINGS);
        return tableService.updateRefreshSettings(id, updateRefreshSettingsRequest);
    }
}
