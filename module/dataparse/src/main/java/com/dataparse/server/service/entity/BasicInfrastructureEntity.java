package com.dataparse.server.service.entity;

import com.dataparse.server.auth.*;
import com.dataparse.server.service.user.*;
import com.dataparse.server.util.hibernate.search.analyzer.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.hibernate.*;
import org.hibernate.annotations.*;
import org.hibernate.annotations.FetchMode;
import org.hibernate.search.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import java.util.*;

@Data
@MappedSuperclass
@Indexed(interceptor = BasicEntityIndexingInterceptor.class)
public abstract class BasicInfrastructureEntity extends BasicEntity {

    @Fetch(org.hibernate.annotations.FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.EAGER, cascade = {CascadeType.DETACH})
    @JsonIgnore
    private User user;

    @Fetch(FetchMode.SELECT)
    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JsonIgnore
    @JoinTable(inverseJoinColumns = @JoinColumn(name = "entity_operation_id", referencedColumnName = "id"))
    @Cascade(org.hibernate.annotations.CascadeType.ALL)
    List<EntityOperation> entityOperation = new ArrayList<>();

    /************************************************
     *
     *   GETTERS BELOW SHOULD BE TRANSIENT:
     *
     * - used for search
     * - provide access to internal fields
     *   of associations that are marked with @JsonIgnore
     *
     ************************************************/

    @Transient
    @JsonIgnore
    public EntityOperation getOrCreateEntityOperation(Session session){
        EntityOperation op = getEntityOperationByMe();
        if(op == null){
            op = EntityOperation.modified(session);
            getEntityOperation().add(op);
        }
        return op;
    }

    @Transient
    @JsonIgnore
    private EntityOperation getEntityOperationByMe(){
        return entityOperation.stream()
                .filter(op -> op.getUser().getId().equals(Auth.get().getUserId()))
                .findFirst()
                .orElse(null);
    }

    @Transient
    public Date getLastViewedByMe(){
        EntityOperation lastOpByMe = getEntityOperationByMe();
        if(lastOpByMe != null){
            return lastOpByMe.getViewed();
        }
        return null;
    }

    @Transient
    public Date getLastModifiedByMe(){
        EntityOperation lastOpByMe = getEntityOperationByMe();
        if(lastOpByMe != null){
            return lastOpByMe.getModified();
        }
        return null;
    }

    @Transient
    public String getUserName(){
        return getUser() == null ? null : getUser().getEmail();
    }

    @Transient
    public Long getUserId(){
        return getUser() == null ? null : getUser().getId();
    }

    @Transient
    @JsonIgnore
    @Field(analyze = Analyze.NO)
    public String getUserIdStr(){
        return getUser() == null ? null : getUser().getId().toString();
    }

    @Transient
    @JsonIgnore
    @Field(name = "keywords", analyzer = @Analyzer(impl = WhitespaceAnalyzer.class))
    public String getKeywords() {
        return null;
    }

}
