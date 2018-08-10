package com.dataparse.server.service.upload;

import com.dataparse.server.service.db.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.*;

@Data
@Entity
@Table(name = "db_descriptor")
@EqualsAndHashCode(callSuper = true, exclude = { "disconnectedTime", "queryHistory" })
public class DbDescriptor extends RemoteLinkDescriptor {

    private Date disconnectedTime;

    @Fetch(FetchMode.SELECT)
    @Cascade(org.hibernate.annotations.CascadeType.ALL)
    @JoinColumn(name = "descriptor", referencedColumnName = "id", nullable = false)
    @OneToMany(fetch= FetchType.EAGER, cascade = {CascadeType.ALL}, orphanRemoval = true)
    private List<DbQueryHistoryItem> queryHistory;

}
