package com.dataparse.server.service.upload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class XlsFormatSettings extends FormatSettings {

    private Boolean useHeaders;
    @Min(1) @Max(65535) private Integer startOnRow = 1;
    @Min(0) @Max(65535) private Integer skipAfterHeader = 0;
    @Min(0) @Max(65535) private Integer skipFromBottom;

}
