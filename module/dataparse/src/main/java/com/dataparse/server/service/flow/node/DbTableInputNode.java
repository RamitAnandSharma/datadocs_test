package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.files.preview.FilePreviewService;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.UploadService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.function.Consumer;

public class DbTableInputNode extends InputNode<DbTableInputNodeSettings> {

    public DbTableInputNode(final String id) {
        super(id);
    }

    @Autowired
    private UploadService uploadService;

    @Override
    protected Descriptor execute(final boolean preview, final Consumer<NodeState> nodeStateConsumer) {
        Upload upload = (Upload) uploadRepository.getFile(getSettings().getTableId());
        if(preview){
            if(upload.getPreviewDescriptor() != null){
                return upload.getPreviewDescriptor();
            } else {
                upload.getDescriptor().setLimit(FilePreviewService.PREVIEW_LIMIT);
            }
        } else {
            Map<AbstractParsedColumn, Object> exampleValues = uploadService.retrieveExampleValues(null, upload.getDescriptor());
            upload.getDescriptor().getColumns().forEach(columnInfo -> {
                Object exampleValue = exampleValues.get(ParsedColumnFactory.getByColumnInfo(columnInfo));
                if(exampleValue != null) {
                    columnInfo.setExampleValue(ColumnInfo.getExampleValueFormatted(columnInfo, exampleValue.toString()));
                }
            });
        }

        return upload.getDescriptor();
    }
}
