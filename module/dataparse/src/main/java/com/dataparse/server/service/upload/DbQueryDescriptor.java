package com.dataparse.server.service.upload;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.annotations.Type;

import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "db_query_descriptor")
@EqualsAndHashCode(callSuper = true)
public class DbQueryDescriptor extends RemoteLinkDescriptor {

    @Lob
    @Type(type = "org.hibernate.type.TextType")
    private String query;

}
