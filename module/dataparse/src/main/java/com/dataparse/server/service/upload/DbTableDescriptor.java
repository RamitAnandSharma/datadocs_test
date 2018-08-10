package com.dataparse.server.service.upload;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.persistence.Entity;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "db_table_descriptor")
@EqualsAndHashCode(callSuper = true)
public class DbTableDescriptor extends RemoteLinkDescriptor {

    private String tableName;

}
