package com.dataparse.server.service.upload;

import com.dataparse.server.service.user.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import java.io.*;
import java.util.*;

@Data
@Entity
public class UploadSession implements Serializable {

    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(
            name = "UUID",
            strategy = "org.hibernate.id.UUIDGenerator")
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    private Date created;

    private String key;

    @Fetch(org.hibernate.annotations.FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.EAGER, cascade = {CascadeType.DETACH})
    @JsonIgnore
    private User user;

}
