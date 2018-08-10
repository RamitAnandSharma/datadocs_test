package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.files.preview.*;
import com.dataparse.server.service.upload.*;

import java.util.function.*;

public class DataSourceInputNode extends InputNode<InputNodeSettings> {

    public DataSourceInputNode(final String id) {
        super(id);
    }

    @Override
    public Descriptor execute(boolean preview, Consumer<NodeState> nodeStateConsumer) {
        Upload upload = (Upload) uploadRepository.getFile(getSettings().getUploadId());
        if(preview){
            if(upload.getPreviewDescriptor() != null){
                return upload.getPreviewDescriptor();
            } else {
                upload.getDescriptor().setLimit(FilePreviewService.PREVIEW_LIMIT);
            }
        }
        return upload.getDescriptor();
    }
}
