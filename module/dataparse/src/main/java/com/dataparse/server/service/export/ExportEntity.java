package com.dataparse.server.service.export;

import com.dataparse.server.service.docs.Datadoc;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

@Data
@Entity
@NoArgsConstructor
public class ExportEntity implements Serializable {

    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(
            name = "UUID",
            strategy = "org.hibernate.id.UUIDGenerator")
    @Column(updatable = false, nullable = false)
    private UUID resultFileId;

    private Date exportDate;

    @ManyToOne(fetch = FetchType.EAGER)
    private Datadoc datadoc;

    private Boolean removed = false;

    private Boolean emailSent = false;
    private Boolean downloaded = false;
    private String sendTo;

    public ExportEntity(UUID resultFileId, Date exportDate, Datadoc datadoc, String sendTo) {
        this.resultFileId = resultFileId;
        this.exportDate = exportDate;
        this.datadoc = datadoc;
        this.sendTo = sendTo;
    }
}
