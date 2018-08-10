package com.dataparse.server.service.entity;

import com.dataparse.server.service.upload.*;
import org.hibernate.search.indexes.interceptor.*;

public class BasicEntityIndexingInterceptor implements EntityIndexingInterceptor<BasicEntity> {

    private boolean isDbTable(BasicEntity entity){
        return (entity instanceof Upload) && (((Upload) entity).getDescriptor() instanceof DbTableDescriptor);
    }

    @Override
    public IndexingOverride onAdd(final BasicEntity entity) {
        if(entity.isDeleted() || isDbTable(entity)){
            return IndexingOverride.REMOVE;
        }
        return IndexingOverride.APPLY_DEFAULT;
    }

    @Override
    public IndexingOverride onUpdate(final BasicEntity entity) {
        if(entity.isDeleted() || isDbTable(entity)){
            return IndexingOverride.REMOVE;
        }
        return IndexingOverride.APPLY_DEFAULT;
    }

    @Override
    public IndexingOverride onDelete(final BasicEntity entity) {
        return IndexingOverride.REMOVE;
    }

    @Override
    public IndexingOverride onCollectionUpdate(final BasicEntity entity) {
        if(entity.isDeleted() || isDbTable(entity)){
            return IndexingOverride.REMOVE;
        }
        return IndexingOverride.APPLY_DEFAULT;
    }
}
