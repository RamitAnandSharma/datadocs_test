package com.dataparse.server.service.upload;

import com.dataparse.server.service.parser.DataFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;

@Data
@Entity
@Table(name = "csv_descriptor")
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CsvFileDescriptor extends FileDescriptor {

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private CsvFormatSettings settings = new CsvFormatSettings();

    @Override
    public DataFormat getFormat() {
        return DataFormat.CSV;
    }

    public CsvFileDescriptor(String path) {
        setPath(path);
    }
}
