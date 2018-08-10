package com.dataparse.server;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.session.JDBCSessionIdManager;
import org.eclipse.jetty.server.session.JDBCSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.util.SystemUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class RestServerBak {
  private static final String CONTEXT_PATH = "/";
  private static final String MAPPING_URL = "/*";
  private static final String CONFIG_LOCATION = "com.dataparse.server.config";
  private static final String PORT = "PORT";
  private static final String SLAVE = "SLAVE";

  public static Integer getServerPort() { return SystemUtils.getProperty(PORT, 9100); }

  public static Boolean isSlave(){ return SystemUtils.getProperty(SLAVE, false); }

  public static Boolean isMaster(){ return !isSlave(); }

  public static void main(String[] args) throws Exception {
    Flyway flyway = new Flyway();
    flyway.setDataSource(AppConfig.getDbUrl(), AppConfig.getDbUrl(), "", "");
    if(AppConfig.getDbRecreate()){
      flyway.clean();
      MigrationInfo[] pending = flyway.info().pending();
      if(pending.length > 0) {
        String version = pending[pending.length - 1].getVersion().getVersion();
        flyway.setBaselineVersionAsString(version);
        flyway.baseline();
      }
    }
    flyway.migrate();

    
    
    Server server = new Server(getServerPort());
    server.setHandler(getServletContextHandler(server, getContext()));
    server.start();
    server.join();
  }

  private static ServletContextHandler getServletContextHandler(Server server, WebApplicationContext context) throws IOException {
    ServletContextHandler contextHandler = new ServletContextHandler();

    contextHandler.setErrorHandler(null);
    contextHandler.setContextPath(CONTEXT_PATH);
    ServletHolder holder = new ServletHolder(new DispatcherServlet(context));
    holder.setInitParameter("cacheControl", "max-age=0, public");
    contextHandler.addServlet(holder, MAPPING_URL);
    contextHandler.addEventListener(new ContextLoaderListener(context));
    JDBCSessionIdManager idMgr = new JDBCSessionIdManager(server);
    idMgr.setWorkerName(String.valueOf(1000 + new Random((new Date()).getTime()).nextInt(8999)));
    idMgr.setDriverInfo("org.postgresql.Driver", AppConfig.getDbUrl());
    idMgr.setScavengeInterval(60);
    server.setSessionIdManager(idMgr);

    JDBCSessionManager sessionManager = new JDBCSessionManager();
    sessionManager.getSessionCookieConfig().setName("dataparse-session");
    sessionManager.setSessionIdManager(idMgr);

    SessionHandler sessionHandler = new SessionHandler(sessionManager);
    contextHandler.setSessionHandler(sessionHandler);

    ErrorHandler errorHandler = new ErrorHandler(){

      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if(HttpStatus.NOT_FOUND_404 == response.getStatus()){
          try {
            request.getRequestDispatcher("/404").forward(request, response);
          } catch (ServletException e) {
            e.printStackTrace();
          }
        } else {
          super.handle(target, baseRequest, request, response);
        }
      }

      @Override
      protected void handleErrorPage(HttpServletRequest request, Writer writer, int code, String message) throws IOException {
        if (message == null)
          message = HttpStatus.getMessage(code);
        ObjectMapper mapper = new ObjectMapper();

        Throwable th = (Throwable)request.getAttribute("javax.servlet.error.exception");
        String[] rootCause = new String[0];
        if(th != null){
          rootCause = ExceptionUtils.getRootCauseStackTrace(th);
        }
        Map response = ImmutableMap.of("message", message, "stackTrace", rootCause);
        mapper.writeValue(writer, response);
      }
    };

    contextHandler.setErrorHandler(errorHandler);
    return contextHandler;
  }

  private static WebApplicationContext getContext() {
    AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
    context.setConfigLocation(CONFIG_LOCATION);
    return context;
  }

}
