package com.dataparse.server.service.upload;

import com.dataparse.server.service.db.ConnectionParams;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToOne;
import java.util.Date;

@Data
@MappedSuperclass
@EqualsAndHashCode(callSuper = true, exclude = { "lastConnected", "lastConnectionTestSuccessful" })
public abstract class RemoteLinkDescriptor extends Descriptor {

    private Date lastConnected;
    private Boolean lastConnectionTestSuccessful;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private ConnectionParams params;
}
