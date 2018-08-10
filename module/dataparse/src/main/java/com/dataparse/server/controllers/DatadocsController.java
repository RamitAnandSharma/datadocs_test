package com.dataparse.server.controllers;

import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.service.docs.*;
import com.dataparse.server.service.schema.*;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.web.bind.annotation.*;

// todo
// model hierarchy has changed, so most of methods were moved to FileController
@RestController
@RequestMapping("/api/docs")
@Api(value = "Datadocs", description = "Operations with indexed tables")
public class DatadocsController extends ApiController {

    @Autowired
    private TableService tableService;

    @ApiOperation(value = "Create datadoc")
    @RequestMapping(method = RequestMethod.POST)
    public Datadoc createDatadoc(@RequestBody CreateDatadocRequest request) {
        return tableService.createDatadoc(request);
    }

    @Deprecated
    @ApiOperation(value = "Delete datadoc")
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteDatadoc(@PathVariable Long id) {
        tableService.removeDatadoc(id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.POST)
    public void updateDatadoc(@PathVariable Long id, @RequestBody UpdateDatadocRequest updateDatadocRequest) {
        tableService.updateDatadoc(id, updateDatadocRequest);
    }

    @ApiOperation(value = "Get statistics")
    @RequestMapping(value = "/{id}/stats", method = RequestMethod.GET)
    public DatadocStatistics getDatadocsStats(@PathVariable Long id) {
        return tableService.getDatadocStatistics(id);
    }
}
