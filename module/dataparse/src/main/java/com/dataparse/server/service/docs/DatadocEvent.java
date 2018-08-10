package com.dataparse.server.service.docs;

import com.dataparse.server.service.notification.Event;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
public abstract class DatadocEvent extends Event {
    @NonNull private Long datadocId;
    private Long tabId;
    private UUID stateId;
    private Integer tabIndex;
    private String renameTo;
}
