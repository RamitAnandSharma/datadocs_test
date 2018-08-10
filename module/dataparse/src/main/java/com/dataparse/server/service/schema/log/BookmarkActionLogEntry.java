package com.dataparse.server.service.schema.log;

import com.dataparse.server.service.engine.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;

import javax.persistence.*;
import java.util.*;

@Data
@Entity
@Table(name = "bookmark_action_log")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "type", discriminatorType=DiscriminatorType.STRING, length = 3)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class BookmarkActionLogEntry {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private Date startTime;
    private Long duration;
    private boolean success;

    private Long userId;
    private Long billableUserId;
    private Long datadocId;
    private Long bookmarkId;
    private Long tableId;

    private Long cost; // in micro-amount

    @Enumerated(EnumType.ORDINAL)
    private EngineType storageType;

}
