package com.dataparse.server.service.files;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.search.annotations.*;

import javax.persistence.AttributeOverride;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;
import java.util.List;
import java.util.Map;

@Data
@Entity
@Indexed
@EqualsAndHashCode(callSuper = true)
public class Folder extends AbstractFile {

    @Override
    public String getEntityType() {
        return "Folder";
    }

    @Override
    public String getType() {
        return "folder";
    }
}
