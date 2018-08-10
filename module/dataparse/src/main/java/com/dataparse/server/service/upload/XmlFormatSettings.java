package com.dataparse.server.service.upload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class XmlFormatSettings extends FormatSettings {

    String rowXPath;

}
