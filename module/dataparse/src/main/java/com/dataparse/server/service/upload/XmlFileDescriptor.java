package com.dataparse.server.service.upload;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;
import java.util.List;


@Data
@Entity
@Table(name = "xml_descriptor")
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class XmlFileDescriptor extends FileDescriptor {

    @Fetch(FetchMode.SUBSELECT)
    @ElementCollection(fetch = FetchType.EAGER)
    private List<String> availableRowXPaths;

    @Fetch(FetchMode.SELECT)
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private XmlFormatSettings settings = new XmlFormatSettings();

}
