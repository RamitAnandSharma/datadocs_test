package com.dataparse.server.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpSession;

@ApiIgnore
@Controller
public class HomeController {

    @RequestMapping("/")
    public String index()
    {
        return "static/index.html";
    }

    @RequestMapping("/404")
    public String notFound(){ return "static/404.html"; }

//    @RequestMapping("/datadocs.com.html")
//    public String tmp(){ return "static/datadocs.com.html"; }

    @RequestMapping(value = "/embed", method = RequestMethod.GET, produces = "text/plain")
    public String embed(HttpSession session)
    {
        return "static/embed.html";
    }

    @RequestMapping(value = "/testresult", method = RequestMethod.GET, produces = "text/plain")
    public String testresult(HttpSession session) {
        return "static/testResult.html";
    }

}
