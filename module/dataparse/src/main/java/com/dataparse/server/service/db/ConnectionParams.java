package com.dataparse.server.service.db;

import com.dataparse.server.service.parser.jdbc.Protocol;
import lombok.*;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"id"})
public class ConnectionParams implements Serializable {

    @Id
    @Column(name="id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Enumerated(EnumType.STRING)
    private Protocol protocol;

    @NotEmpty
    private String host;

    @NotNull @Range(min = 1, max = 65535)
    private Integer port;

    @NotEmpty
    private String user;

    @NotEmpty
    private String password;

    @NotEmpty
    private String dbName;

//    private Map<String, String> params;


    @Override
    public String toString() {
        return "ConnectionParams{" +
               "protocol=" + protocol +
               ", host='" + host + '\'' +
               ", port=" + port +
               ", user='" + user + '\'' +
               ", password='" + password + '\'' +
               ", dbName='" + dbName + '\'' +
               '}';
    }
}
