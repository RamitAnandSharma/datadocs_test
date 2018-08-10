package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.flow.*;
import com.dataparse.server.service.upload.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;

public abstract class InputNode<T extends InputNodeSettings> extends Node<T> {

    @Autowired
    protected UploadRepository uploadRepository;

    public InputNode(String id) {
        super(id);
    }

    @Override
    public List<FlowValidationError> getValidationErrors() {
        List<FlowValidationError> errors = super.getValidationErrors();
        Upload upload = (Upload) uploadRepository.getFile(getSettings().getUploadId());
        if(upload == null || upload.isDeleted()){
            errors.add(new FlowValidationError(getId(), "source_not_found", "Can't find Data Source " + getNodeName()));
        }
        else if(!upload.getDescriptor().isValid() && !upload.getDescriptor().isRemote()) {
            errors.add(new FlowValidationError(getId(), upload.getDescriptor().getErrorCode(), upload.getDescriptor().getErrorString()));
        }
        return errors;
    }

    @Override
    protected String getNodeTypeName() {
        return "Input";
    }

    @Override
    protected int getExpectedChildrenCount() {
        return 0;
    }
}
