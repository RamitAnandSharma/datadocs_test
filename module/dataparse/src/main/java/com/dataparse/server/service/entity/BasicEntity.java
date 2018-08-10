package com.dataparse.server.service.entity;

import lombok.*;
import org.hibernate.search.annotations.*;

import javax.persistence.*;
import java.io.*;
import java.util.*;

@Data
@MappedSuperclass
public abstract class BasicEntity implements Serializable {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private Date created;
    private Date updated;

    private boolean deleted;

    @Column(name = "note")
    @Enumerated(EnumType.ORDINAL)
    @ElementCollection(targetClass = StickyNoteType.class, fetch = FetchType.EAGER)
    @CollectionTable(name = "closed_notes", joinColumns = @JoinColumn(name = "entity_id"), foreignKey = @ForeignKey(ConstraintMode.NO_CONSTRAINT))
    Set<StickyNoteType> closedNotes = new HashSet<>();

}
