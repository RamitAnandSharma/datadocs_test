package com.dataparse.server.controllers.api.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotBlank;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateDatadocRequest {

    @NotBlank
    String name;
    Boolean preSave;
    Boolean embedded = false;

}
