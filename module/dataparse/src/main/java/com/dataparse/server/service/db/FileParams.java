package com.dataparse.server.service.db;

import lombok.Data;
import java.io.Serializable;

@Data
public class FileParams implements Serializable {
    private String name;
}
