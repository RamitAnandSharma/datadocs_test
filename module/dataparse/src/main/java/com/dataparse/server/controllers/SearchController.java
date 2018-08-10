package com.dataparse.server.controllers;


import com.dataparse.server.auth.*;
import com.dataparse.server.controllers.api.search.*;
import com.dataparse.server.service.files.*;
import com.dataparse.server.service.upload.*;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/search")
@Api(value = "Search", description = "Search files and docs")
public class SearchController extends ApiController {

    @Autowired
    private UploadRepository uploadRepository;

    @ApiOperation("Refresh index")
    @RequestMapping("/refresh")
    public void refresh(){
        uploadRepository.refreshIndex();
    }

    @ApiOperation("Auto-suggest files, folders and docs")
    @RequestMapping(value = "/suggest", method = RequestMethod.POST)
    public List<AbstractFile> suggest(@RequestBody SearchRequest searchRequest){
        return uploadRepository.autoSuggestFilesAndDocs(Auth.get().getUserId(), searchRequest.getS(), searchRequest.getLimit());
    }

    @ApiOperation("Search files and folders")
    @RequestMapping(value = "/suggest-sources", method = RequestMethod.POST)
    public List<AbstractFile> suggestFiles(@RequestBody SearchRequest searchRequest){
        return uploadRepository.autoSuggestSources(Auth.get().getUserId(), searchRequest.getS(), searchRequest.getLimit());
    }

    @ApiOperation("Search files, folder and docs")
    @RequestMapping(method = RequestMethod.POST)
    public SearchResponse search(@RequestBody SearchRequest searchRequest){
        return uploadRepository.searchFilesAndDocs(Auth.get().getUserId(), searchRequest);
    }

}
