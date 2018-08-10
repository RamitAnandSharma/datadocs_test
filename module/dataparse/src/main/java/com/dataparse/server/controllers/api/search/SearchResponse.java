package com.dataparse.server.controllers.api.search;

import com.dataparse.server.service.files.*;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchResponse {

    private List<AbstractFile> files;

}
