package com.dataparse.server.service.engine;

import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.util.*;

public enum EngineSelectionStrategy {
    ALWAYS_BQ,
    ALWAYS_ES,
    DEPENDING_ON_DATASET_SIZE;

    private static final String ENGINE = "ENGINE";
    private static final String ALLOW_MANUAL_ENGINE_SELECTION = "ALLOW_MANUAL_ENGINE_SELECTION";

    public static boolean isAllowManualSelection(){
        return SystemUtils.getProperty(ALLOW_MANUAL_ENGINE_SELECTION, false);
    }

    public static EngineSelectionStrategy current(){
        return EngineSelectionStrategy.valueOf(
                SystemUtils.getProperty(ENGINE, EngineSelectionStrategy.DEPENDING_ON_DATASET_SIZE.name()));
    }

    public EngineType getEngineType(Descriptor desc){
        EngineType engineType = null;
        switch (this){
            case DEPENDING_ON_DATASET_SIZE:
                // decide depending on rows count
                if(desc.getRowsCount() != null){
                    if (desc.getRowsCount() < 1E7
                            || desc.getFormat().equals(DataFormat.XML)) {
                        engineType = EngineType.ES;
                    } else {
                        engineType = EngineType.BIGQUERY;
                    }
                } else {
                    engineType = EngineType.ES;
                }
                break;
            case ALWAYS_BQ:
                engineType = EngineType.BIGQUERY;
                break;
            case ALWAYS_ES:
                engineType = EngineType.ES;
                break;
            default:
                throw new RuntimeException("Unknown engine selection strategy: " + this);
        }
        return engineType;
    }
}
