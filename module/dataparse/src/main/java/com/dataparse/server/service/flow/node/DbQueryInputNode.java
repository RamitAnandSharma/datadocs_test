package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.db.DbQueryHistoryItem;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.preview.FilePreviewService;
import com.dataparse.server.service.flow.FlowValidationError;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.util.DbUtils;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class DbQueryInputNode extends InputNode<DbQueryInputNodeSettings> {

    @Autowired
    private FilePreviewService filePreviewService;

    private DbQueryHistoryItem queryHistoryItem;

    public DbQueryInputNode(final String id) {
        super(id);
    }

    @Override
    public List<FlowValidationError> getValidationErrors() {
        List<FlowValidationError> errors = super.getValidationErrors();
        if(StringUtils.isBlank(getSettings().getQuery())) {
            errors.add(new FlowValidationError(getId(), "Query is empty"));
        }
        return errors;
    }

    private DbQueryDescriptor createNewQueryDescriptor(ConnectionParams connectionParams, String query){
        DbQueryDescriptor queryDescriptor = new DbQueryDescriptor();
        queryDescriptor.setColumns(new ArrayList<>());
        queryDescriptor.setParams(connectionParams);
        queryDescriptor.setQuery(query);
        queryDescriptor.setFormat(DbUtils.getRemoteDataSourceFormat(connectionParams.getProtocol()));
        return queryDescriptor;
    }

    @Override
    protected Descriptor execute(final boolean preview, final Consumer consumer) {
        Stopwatch execution = Stopwatch.createStarted();
        AbstractFile obj = uploadRepository.getFile(getSettings().getUploadId());
        // todo simplify?
        if(obj == null
           || !(obj instanceof Upload)
           || !(((Upload) obj).getDescriptor() instanceof DbDescriptor)) {
            throw new RuntimeException("Remote data source not found");
        }
        Upload u = (Upload) obj;
        DbDescriptor dbDescriptor = (DbDescriptor) u.getDescriptor();
        DbQueryDescriptor queryDescriptor = createNewQueryDescriptor(dbDescriptor.getParams(), getSettings().getQuery());

        Descriptor copyDescriptor;
        Date startTime = new Date();
        boolean success = false;
        try {
            copyDescriptor = filePreviewService.copy(preview, queryDescriptor, true);
            success = true;
        } finally {
            DbQueryHistoryItem historyItem = new DbQueryHistoryItem();
            historyItem.setStartTime(startTime);
            historyItem.setDuration(new Date().getTime() - startTime.getTime());
            historyItem.setQuery(queryDescriptor.getQuery());
            historyItem.setSuccess(success);
            uploadRepository.saveQueryHistoryItem(dbDescriptor.getId(), historyItem);
            this.queryHistoryItem = historyItem;
        }

        for(ColumnInfo columnInfo : copyDescriptor.getColumns()){
            columnInfo.setId((long) ParsedColumnFactory.getByColumnInfo(columnInfo).hashCode());
        }
        log.info("Run db query input node took {}", execution.stop());

        if(preview){
            return copyDescriptor;
        }
        queryDescriptor.setColumns(copyDescriptor.getColumns());
        return queryDescriptor;
    }

    public DbQueryHistoryItem getQueryHistoryItem() {
        return queryHistoryItem;
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !(o instanceof DbQueryInputNode)) {
            return false;
        }
        if(o == this) {
            return true;
        }
        DbQueryInputNode that = (DbQueryInputNode) o;
        return that.getSettings().getQuery().equals(this.getSettings().getQuery()) && that.getId().equals(this.getId());
    }
}
