package com.dataparse.server.service.docs;

import com.dataparse.server.service.files.*;
import com.dataparse.server.service.files.dto.SharedWithDTO;
import com.dataparse.server.service.user.User;
import com.dataparse.server.util.hibernate.search.analyzer.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import lombok.*;
import org.hibernate.search.annotations.*;

import javax.persistence.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Entity
@Indexed
@EqualsAndHashCode(callSuper = true)
public class Datadoc extends AbstractFile {

    @Fields({
        @Field,
        @Field(name = "exact_name", analyzer = @Analyzer(impl = KeywordAnalyzer.class))
    })
    private String name;

    private long tabCounter = 0;

    private Date committed;

    private Boolean publicShared = false;

    public Boolean getPublicShared() {
        if(publicShared == null) {
            return false;
        }
        return publicShared;
    }

    private UUID shareId;

    private UUID sharedStateId = null;

    @JsonIgnore
    @ManyToOne(fetch = FetchType.EAGER)
    private Datadoc parentDatadoc;

    /**
     * the first level datadoc, in graph view, it's like parent node for each other
     * */

    @JsonIgnore
    @ManyToOne(fetch = FetchType.EAGER)
    private Datadoc sourceDatadoc;

    @JsonIgnore
    @OneToMany(mappedBy = "primaryKey.datadoc", fetch = FetchType.EAGER)
    private List<UserFileShare> sharedWith;

    @Transient
    @JsonIgnore
    public List<User> getSharedWith() {
        return this.sharedWith == null
                ? Lists.newArrayList()
                : this.sharedWith.stream().map(UserFileShare::getUser).collect(Collectors.toList());
    }

    @Transient
    public SharedWithDTO getSharedWithInfo() {
        return new SharedWithDTO(getSharedUsers());
    }

    @Transient
    @JsonIgnore
    public List<User> getSharedWithOwner() {
        List<User> users = new ArrayList<>(getSharedWith());
        users.add(getUser());
        return users;
    }

    @Transient
    @JsonIgnore
    public Map<Long, UserFileShare> getSharedWithById() {
        return sharedWith == null
                ? new HashMap<>()
                : sharedWith.stream().collect(Collectors.toMap(u -> u.getUser().getId(), Function.identity()));
    }

    @Transient
    @JsonIgnore
    public List<UserFileShare> getSharedUsers() {
        return sharedWith == null
                ? Lists.newArrayList()
                : sharedWith;
    }

    @Transient
    private List<String> lastFlowExecutionTasks = new ArrayList<>();

    @Transient
    private String gathererTask;


    @Override
    public String getEntityType() {
        return "Datadoc";
    }

    @Override
    public String getType() {
        return "doc";
    }
}
