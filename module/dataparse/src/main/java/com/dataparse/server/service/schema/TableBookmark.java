package com.dataparse.server.service.schema;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.service.docs.*;
import com.dataparse.server.service.embed.*;
import com.dataparse.server.service.user.User;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.dto.BookmarkStateViewDTO;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.Lists;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import java.util.*;

@Data
@Entity
public class TableBookmark {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Fetch(FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.EAGER, optional = false, cascade = {CascadeType.DETACH})
    @JsonIgnore
    private Datadoc datadoc;

    @Fetch(FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.EAGER, optional = false, cascade = {CascadeType.DETACH})
    private TableSchema tableSchema;

    private UUID defaultState;

    @Transient
    private UUID currentState;
    @Transient
    private BookmarkState state;

    @Transient
    private List<BookmarkStateViewDTO> allBookmarkStates;

    @Transient
    public UUID getCurrentState() {
        if(currentState == null) {
            return defaultState;
        }
        return currentState;
    }


    @Transient
    @JsonIgnore
    public void addBookmarkState(BookmarkState bookmarkState) {
        if(allBookmarkStates == null) {
            allBookmarkStates = Lists.newArrayList();
        }
        allBookmarkStates.add(new BookmarkStateViewDTO(bookmarkState));
    }

    @Transient
    public List<BookmarkStateViewDTO> getAllBookmarkStates() {
        return allBookmarkStates == null ? Lists.newArrayList() : allBookmarkStates;
    }

    @Fetch(FetchMode.JOIN)
    @OneToOne(fetch = FetchType.EAGER, cascade = {CascadeType.ALL})
    private EmbedSettings embedSettings;

    @Transient
    public BookmarkStateId getBookmarkStateId() {
        return new BookmarkStateId(getId(), getCurrentState(), Auth.get().getUserId());
    }

    private int position;
    private String name;

    private Date created;
    private Date updated;

    @Fetch(FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.LAZY, optional = false, cascade = {CascadeType.DETACH})
    private User createdByUser;
}
