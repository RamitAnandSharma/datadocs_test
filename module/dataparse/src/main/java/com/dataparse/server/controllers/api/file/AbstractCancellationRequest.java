package com.dataparse.server.controllers.api.file;

import lombok.Data;

import javax.persistence.Transient;
import java.io.Serializable;

@Data
public class AbstractCancellationRequest implements Serializable {
    @Transient
    private Boolean cancel = false;
}
