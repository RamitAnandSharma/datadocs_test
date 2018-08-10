package com.dataparse.server.service.schema;

import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.user.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.hibernate.annotations.*;
import org.hibernate.search.annotations.*;

import javax.persistence.CascadeType;
import javax.persistence.*;
import javax.persistence.Entity;
import java.util.*;

@Data
@Entity
@EqualsAndHashCode
public class TableSchema {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Deprecated private String pkName;
    private Date firstCommitted;
    private Date committed;

    private Long lastCommitDuration;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private Descriptor descriptor;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private RefreshSettings refreshSettings;

    @JsonIgnore
    private String externalId;

    private String accountId;

    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(name = "table_uploads",
               joinColumns = @JoinColumn(name = "table_schema", referencedColumnName = "id"),
               inverseJoinColumns = @JoinColumn(name = "uploads", referencedColumnName = "id"))
    private List<Upload> uploads = new ArrayList<>();

    private EngineType engineType;

    private Long totalIngested;

    @Override
    public String toString() {
        return "TableSchema [" + getId() + ", pkey=" + pkName + ", refresh=" + refreshSettings + "]";
    }
}
