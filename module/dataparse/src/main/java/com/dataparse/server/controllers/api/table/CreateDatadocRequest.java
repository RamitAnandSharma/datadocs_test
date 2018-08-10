package com.dataparse.server.controllers.api.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;
import org.hibernate.validator.constraints.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateDatadocRequest {

  public CreateDatadocRequest(final String name) {
    this.name = name;
  }

  @NotBlank
  String name;
  Long parentId;
  String sourcePath;
  Boolean preSave = false;
  Boolean embedded = false;
  //  it's the optional field, that define the time for watch the source has been processed.
  //  it's can be changed if implement files pre-processing through the tasks .
  Long preProcessionTime; // millis

  boolean autoIngest;

}
