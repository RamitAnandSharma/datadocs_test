package com.dataparse.server.controllers;

import com.dataparse.server.auth.*;
import com.dataparse.server.service.schema.log.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/billing")
public class BillingController extends ApiController {

    @Autowired
    private BookmarkActionLogRepository actionLogRepository;

    @RequestMapping(value = "/events", method = RequestMethod.GET)
    public List<BookmarkActionLogEntry> getMyEvents(){
        return actionLogRepository.getUserEvents(Auth.get().getUserId());
    }

}
