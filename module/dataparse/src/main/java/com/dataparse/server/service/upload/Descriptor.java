package com.dataparse.server.service.upload;

import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.flow.transform.*;
import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.parser.type.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.CascadeType;
import javax.persistence.*;
import javax.persistence.Entity;
import java.io.*;
import java.util.*;


@Data
@Entity
@Inheritance(strategy=InheritanceType.JOINED)
@DiscriminatorColumn(
        name="discr",
        discriminatorType=DiscriminatorType.STRING
)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(exclude = {"id", "rowsEstimatedCount", "section", "composite", "rowsExactCount", "valid", "firstRows", "errorCode",
                              "errorString", "engine", "columnTransforms", "limit", "options"})
public abstract class Descriptor implements Serializable {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private boolean composite;
    private boolean section;
    private DataFormat format;
    private Long rowsEstimatedCount;
    private Long rowsExactCount;
    private boolean valid = true;

    @Transient
    @JsonIgnore
//    this field is exclusively for inner usage. It's help to avoid parsing overhead.
    private List<Map> firstRows;

    private String errorCode;
    @Lob
    private String errorString;

    private EngineType engine;

    @Transient
    public Long getRowsCount(){
        return rowsExactCount == null ? rowsEstimatedCount : rowsExactCount;
    }

    @Transient
    private List<ColumnTransform> columnTransforms = new ArrayList<>();

    private Integer limit;

    public void setFormat(DataFormat format){
        this.format = format;
        this.composite = isComposite();
        this.section = isSection();
    }

    @Fetch(FetchMode.SUBSELECT)
    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinTable(name = "descriptor_columns",
               joinColumns = @JoinColumn(name = "descriptor", referencedColumnName = "id"),
               inverseJoinColumns = @JoinColumn(name = "columns", referencedColumnName = "id"))
    private List<ColumnInfo> columns;

    public boolean isComposite(){
        return format != null && format.options().isComposite();
    }

    public boolean isSection(){
        return format != null && format.options().isSection();
    }

    @Transient
    public boolean isRemote() { return this instanceof RemoteLinkDescriptor; }

    public void setRemote(boolean remote){
        // empty
    }

    @Transient
    @JsonIgnore
    public String getKeywords() {
        return format != null ? format.options().keywords() : null;
    }

    @Transient
    @JsonIgnore
    public boolean isCacheable(){
        return true;
    }

    @Transient
    @JsonIgnore
    public boolean isUseTuplewiseTransform() {
//        switch (getFormat()) {
//            case JSON_ARRAY:
//            case JSON_LINES:
//            case JSON_OBJECT:
//                return true;
//        }
        return false;
    }

    @Transient
    private Map<String, Object> options = new HashMap<>();
}
