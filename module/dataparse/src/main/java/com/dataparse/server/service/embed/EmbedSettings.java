package com.dataparse.server.service.embed;

import com.dataparse.server.service.schema.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.Entity;

@Data
@Entity
@ToString(exclude = {"tableBookmark"})
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmbedSettings {

    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @JsonIgnore
    @OneToOne(mappedBy = "embedSettings")
    private TableBookmark tableBookmark;

    private String uuid;

    @Column(length = 1024)
    private String title;
    @Lob @Type(type = "org.hibernate.type.TextType")
    private String description;

    private int width = 500;
    private int height = 300;

    private boolean export = false;
    private boolean measures = false;
    private boolean dateFilter = false;
//    private boolean filters = true;
    private boolean search = true;
    private boolean clickIn = false;
    private boolean multiAxis = true;

    @Transient
    public Long getTabId(){
        return tableBookmark == null ? null : tableBookmark.getId();
    }
}
