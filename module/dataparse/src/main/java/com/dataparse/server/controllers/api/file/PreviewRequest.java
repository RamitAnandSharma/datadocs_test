package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.upload.FormatSettings;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.Valid;
import javax.validation.constraints.Max;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PreviewRequest {

    @NotBlank
    String path;

    @Valid
    FormatSettings settings;

    @Max(100000)
    int limit;

}
