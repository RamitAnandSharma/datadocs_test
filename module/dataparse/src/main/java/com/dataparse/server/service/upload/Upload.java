package com.dataparse.server.service.upload;

import com.dataparse.server.service.docs.*;
import com.dataparse.server.service.files.File;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.internal.util.SerializationHelper;
import org.hibernate.search.annotations.*;

import javax.persistence.*;
import javax.persistence.Entity;
import java.util.List;

@Data
@Entity
@Indexed
@EqualsAndHashCode(callSuper = true)
public class Upload extends File {

    private Integer sectionsSize;

    @Transient
    private List<Upload> sections;

    private String checksum;

    @Transient
    private boolean expanded;

    @Transient
    private List<Datadoc> relatedDatadocs;

    @Transient
    private int getRelatedDatadocsCount() {
        return relatedDatadocs != null ? relatedDatadocs.size() : 0;
    }

    @Transient
    @JsonIgnore
    @JsonProperty("embeddedDatadoc")
    private Datadoc getEmbeddedDatadoc() {
        return relatedDatadocs == null ? null : relatedDatadocs.stream().filter(file -> {
            Boolean embedded = file.getEmbedded();
            return embedded != null && embedded;
        }).findFirst().orElse(null);
    }

    public Upload copy() {
        return (Upload) SerializationHelper.clone(this);
    }

    @Override
    public String getType() {
        if (getDescriptor().isComposite()) {
            return "composite-ds";
        } else if (getDescriptor().isSection()) {
            return "section-ds";
        }
        return "ds";
    }

    @Override
    public String toString() {
        return "Upload{" +
                "id=" + getId() +
                ", name='" + getName() + '\'' +
                ", descriptor='" + (getDescriptor() == null ? null : getDescriptor().toString()) + '\'' +
                ", uploaded=" + getUploaded() +
                '}';
    }
}
