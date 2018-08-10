package com.dataparse.server.service.upload;

import com.dataparse.server.service.parser.DataFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.util.List;

@Data
@Entity
@Table(name = "composite_descriptor")
@NoArgsConstructor
@AllArgsConstructor
public class CompositeDescriptor extends Descriptor {

    @Transient
    private List<Descriptor> descriptors;

    @Override
    public DataFormat getFormat() {
        return DataFormat.COMPOSITE;
    }

    @Override
    public boolean isCacheable() {
        return false;
    }
}
