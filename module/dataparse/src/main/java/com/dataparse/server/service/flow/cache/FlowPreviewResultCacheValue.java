package com.dataparse.server.service.flow.cache;

import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.upload.CollectionDelegateDescriptor;
import lombok.*;

import java.io.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlowPreviewResultCacheValue implements Serializable {

    private String flowJSON;
    private FlowSettings settings;
    private CollectionDelegateDescriptor descriptor;

}
