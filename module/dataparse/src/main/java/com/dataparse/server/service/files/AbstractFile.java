package com.dataparse.server.service.files;

import com.dataparse.server.service.entity.*;
import com.dataparse.server.util.hibernate.search.analyzer.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.search.annotations.*;

import javax.persistence.*;
import java.util.*;

@Data
@Entity
@Inheritance(strategy=InheritanceType.JOINED)
@DiscriminatorColumn(
        name="discr",
        discriminatorType=DiscriminatorType.STRING
)
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractFile extends BasicInfrastructureEntity {

    @Fields({
        @Field,
        @Field(name = "exact_name", analyzer = @Analyzer(impl = KeywordAnalyzer.class))
    })
    private String name;

    @Fetch(FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.EAGER, cascade = {CascadeType.DETACH})
    @JsonIgnore
    private AbstractFile parent;

    @Lob
    private String annotation;

    @Access(AccessType.PROPERTY)
    private String entityType;

    @Column(nullable = false)
    private Boolean preSaved = false;

    /**
     * embedded means, that file is for inner usage, and shouldn't be shown in the plane list
     * */
    @Column(nullable = false)
    private Boolean embedded = false;

    public abstract String getEntityType();

    @Transient
    public Long getParentId(){
        if(parent == null){
            return null;
        }
        return parent.getId();
    }

    public void setParentId(){
        // this is for correct deserialization. other ways cause more problems
    }

    @Transient
    public String getParentName(){
        return parent == null ? null : parent.getName();
    }

    public void setParentName(){
        // this is for correct deserialization. other ways cause more problems
    }

    @Transient
    public List<Map<String, Object>> parentsPath;

    @Transient
    public Date getLastViewedByMeOrAddedOn(){
        return getType().equals("doc") ? getLastViewedByMe() : getCreated();
    }

    @Transient
    @Field(name = "type", analyzer = @Analyzer(impl = KeywordAnalyzer.class))
    abstract public String getType();

}
