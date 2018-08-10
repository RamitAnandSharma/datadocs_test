package com.dataparse.server.service.entity;

import com.dataparse.server.auth.*;
import com.dataparse.server.service.user.*;
import lombok.*;
import org.hibernate.*;
import org.hibernate.annotations.*;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import java.io.*;
import java.util.*;

@Data
@Entity
@NoArgsConstructor
public class EntityOperation implements Serializable {

    private static EntityOperation create(Session session, boolean modified){
        Auth auth = Auth.get();
        if(auth.getUserId() != null){
            User user = (User) session.get(User.class, auth.getUserId());
            EntityOperation entityOperation = new EntityOperation();
            entityOperation.setUser(user);
            Date date = new Date();
            entityOperation.setViewed(date);
            if(modified){
                entityOperation.setModified(date);
            }
            return entityOperation;
        }
        return null;
    }

    static EntityOperation viewed(Session session){
        return EntityOperation.create(session, false);
    }

    static EntityOperation modified(Session session){
        return EntityOperation.create(session, true);
    }

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Fetch(FetchMode.SELECT)
    @ManyToOne(fetch = FetchType.EAGER, cascade = {CascadeType.DETACH})
    private User user;

    private Date viewed;
    private Date modified;

}
