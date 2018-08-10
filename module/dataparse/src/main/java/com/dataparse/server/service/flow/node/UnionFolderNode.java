package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.files.*;
import com.dataparse.server.service.flow.*;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.upload.*;
import org.springframework.beans.factory.annotation.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * COMPOSITE Node
 * Concatenates multiple DS of same folder into single descriptor.
 */
public class UnionFolderNode extends Node<UnionFolderNodeSettings>{

    @Autowired
    UploadRepository uploadRepository;

    @Autowired
    ParserFactory parserFactory;

    public UnionFolderNode(String id) {
        super(id);
    }

    public List<File> getUnionFiles(){
        List<File> result = new ArrayList<>();
        List<AbstractFile> files = uploadRepository.getFiles(getFlow().getUserId(), getSettings().getFolderId());
        for(AbstractFile file: files){
            if(file instanceof File){
                result.add((File) file);
            }
        }
        return result;
    }

    @Override
    public List<FlowValidationError> getValidationErrors() {
        List<FlowValidationError> errors = super.getValidationErrors();
        if(getUnionFiles().isEmpty()){
            errors.add(new FlowValidationError(getId(), "Union folder has no data sources!"));
        }
        return errors;
    }

    @Override
    protected Descriptor execute(boolean preview, Consumer<NodeState> nodeStateConsumer) {
        List<Descriptor> descriptors = getUnionFiles()
                .stream().map(File::getDescriptor)
                .collect(Collectors.toList());
        CompositeDescriptor descriptor = new CompositeDescriptor();
        descriptor.setDescriptors(descriptors);
        descriptor.setRowsExactCount(descriptors.stream().mapToLong(Descriptor::getRowsCount).sum());
        descriptor.setColumns(ColumnInfo.aggregateColumns(descriptors));
        return descriptor;
    }

    @Override
    protected String getNodeTypeName() {
        return "Union Folder";
    }

    @Override
    protected int getExpectedChildrenCount() {
        return 0;
    }
}
