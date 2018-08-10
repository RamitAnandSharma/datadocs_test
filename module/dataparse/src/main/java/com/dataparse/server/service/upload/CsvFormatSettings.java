package com.dataparse.server.service.upload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static com.dataparse.server.service.parser.CSVParser.NULL_STRING_DEFAULT;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class CsvFormatSettings extends FormatSettings {

    private String charset;
    private Boolean useHeaders;
    @Min(1) private Integer startOnRow;
    @Min(0) private Integer skipAfterHeader;
    @Min(0) @Max(1000) private Integer skipFromBottom;
    private Character delimiter;
    private Character quote;
    private Character escape;
    private Character commentCharacter;
    @NotNull
    private String nullString = NULL_STRING_DEFAULT;
    private String rowDelimiter;
}
