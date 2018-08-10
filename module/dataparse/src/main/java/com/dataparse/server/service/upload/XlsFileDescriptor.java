package com.dataparse.server.service.upload;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;

@Data
@Entity
@Table(name = "xls_file_descriptor")
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class XlsFileDescriptor extends FileDescriptor {
    private Long sheetIndex;
    private String sheetName;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private XlsFormatSettings settings = new XlsFormatSettings();

    public XlsFileDescriptor(String path) {
        super(path);
    }
}
