package com.dataparse.server.service.files;

import com.dataparse.server.service.upload.Descriptor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
public class File extends AbstractFile {

    private Date uploaded;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private Descriptor descriptor;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private Descriptor previewDescriptor;

    @Override
    public String getType() {
        return "file";
    }

    public String getKeywords() {
        if(descriptor != null){
            return descriptor.getKeywords();
        }
        return null;
    }

    @Override
    public String getEntityType() {
        if(descriptor == null || descriptor.getFormat() == null){
            return null;
        }
        return descriptor.getFormat().options().name();
    }
}
