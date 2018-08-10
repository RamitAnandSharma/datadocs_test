package com.dataparse.server.config;

import com.dataparse.server.service.storage.StorageStrategyType;
import com.dataparse.server.util.SystemUtils;
import com.dataparse.server.util.hibernate.SnakeCaseNamingStrategy;
import com.fasterxml.classmate.TypeResolver;
import com.google.auth.oauth2.GoogleCredentials;
import com.sendgrid.SendGrid;
import org.apache.commons.dbcp.BasicDataSource;
import org.hibernate.SessionFactory;
import org.hibernate.boot.model.naming.ImplicitNamingStrategyLegacyHbmImpl;
import org.hibernate.cfg.AvailableSettings;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.spring4.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.templateresolver.ITemplateResolver;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.builders.ResponseMessageBuilder;
import springfox.documentation.schema.ModelRef;
import springfox.documentation.schema.WildcardType;
import springfox.documentation.service.ApiKey;
import springfox.documentation.service.AuthorizationScope;
import springfox.documentation.service.SecurityReference;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.contexts.SecurityContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger.web.SecurityConfiguration;
import springfox.documentation.swagger.web.UiConfiguration;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static springfox.documentation.schema.AlternateTypeRules.newRule;

@EnableCaching
@EnableAsync
@EnableTransactionManagement
@Configuration
@EnableSwagger2
public class AppConfig extends WebMvcConfigurerAdapter {

  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_NAME = "DB_NAME";
  public static final String DB_RECREATE = "DB_RECREATE";
  public static final String DB_USERNAME = "DB_USERNAME";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  public static final String STORAGE = "STORAGE";
  public static final String BQ_ENABLE_QUERY_CACHE = "BQ_ENABLE_QUERY_CACHE";

  public final static String BQ_PROJECT_ID = "BQ_PROJECT_ID";
  public final static String GOOGLE_APP_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

  public static final String MAIL_API_KEY = "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";

  public static final String URL_WITHOUT_PROTOCOL = SystemUtils.getProperty("SERVER_URL", "localhost");
  public static final String BASE_URL = SystemUtils.getProperty("BASE_URL", "https://" + SystemUtils.getProperty("SERVER_URL", URL_WITHOUT_PROTOCOL));
  public static final String SYSTEM_URL = BASE_URL + "/#";

  private static String getDbHost(){
    return SystemUtils.getProperty(DB_HOST, "localhost");
  }

  private static String getDbPort() {
    return SystemUtils.getProperty(DB_PORT, "5432");
  }

  private static String getDbName(){
    return SystemUtils.getProperty(DB_NAME, "dataparse");
  }

  private static String getDbUsername(){
    return SystemUtils.getProperty(DB_USERNAME, "user");
  }

  private static String getDbPassword(){
    return SystemUtils.getProperty(DB_PASSWORD, "user");
  }

  public static StorageStrategyType getStorageStrategyType() {
    return StorageStrategyType.valueOf(SystemUtils.getProperty(STORAGE, StorageStrategyType.ALWAYS_LOCAL.name()));
  }

  public static boolean getDbRecreate(){ return SystemUtils.getProperty(DB_RECREATE, false); }

  public static String getDbUrl(){
    return "jdbc:postgresql://"
        + AppConfig.getDbHost() + ":" + AppConfig.getDbPort() + "/"
        + AppConfig.getDbName()
        + "?user=" + AppConfig.getDbUsername()
        + "&password=" + AppConfig.getDbPassword();
  }

  public static boolean getBqEnableQueryCache(){
    return SystemUtils.getProperty(BQ_ENABLE_QUERY_CACHE, true);
  }


  public static String getGoogleAppCredentialsFolderPath(){
    return SystemUtils.getProperty(GOOGLE_APP_CREDENTIALS, "deploy/files/production/google_account_keys");
  }

  public static Map<String, GoogleCredentials> getGoogleAppCredentials(){
    File credentialsFolder = new File(AppConfig.getGoogleAppCredentialsFolderPath());
    File[] files = credentialsFolder.listFiles();
    if(files == null){
      throw new RuntimeException("Illegal credentials folder path: " + credentialsFolder.getAbsolutePath());
    }
    return Arrays.stream(files).collect(Collectors.toMap(
        f -> org.apache.commons.io.FilenameUtils.getBaseName(f.getName()), f -> {
          try {
            return GoogleCredentials.fromStream(
                new FileInputStream(f));
          } catch (IOException e) {
            throw new RuntimeException("Can't init Google credentials", e);
          }
        }));
  }

  @Bean
  public CacheManager cacheManager() {
    SimpleCacheManager cacheManager = new SimpleCacheManager();
    cacheManager.setCaches(Collections.singletonList(new ConcurrentMapCache("shareWithUsers")));

    cacheManager.initializeCaches();
    return cacheManager;
  }

  @Bean
  public ViewResolver getViewResolver(){
    InternalResourceViewResolver resolver = new InternalResourceViewResolver();
    resolver.setRedirectHttp10Compatible(false);
    return resolver;
  }

  @Bean
  public LocalSessionFactoryBean sessionFactory() {
    LocalSessionFactoryBean sessionFactory = new LocalSessionFactoryBean();
    sessionFactory.setDataSource(restDataSource());
    sessionFactory.setPackagesToScan("com.dataparse.server.service");
    sessionFactory.setHibernateProperties(hibernateProperties());
    sessionFactory.setPhysicalNamingStrategy(new SnakeCaseNamingStrategy());
    sessionFactory.setImplicitNamingStrategy(new ImplicitNamingStrategyLegacyHbmImpl());
    return sessionFactory;
  }

  @Bean
  public DataSource restDataSource() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("org.postgresql.Driver");
    dataSource.setUrl("jdbc:postgresql://" + getDbHost() + ":" + getDbPort() + "/" + getDbName());
    dataSource.setUsername(getDbUsername());
    dataSource.setPassword(getDbPassword());

    return dataSource;
  }

  @Bean
  @Autowired
  public HibernateTransactionManager transactionManager(SessionFactory sessionFactory) {
    HibernateTransactionManager txManager = new HibernateTransactionManager();
    txManager.setSessionFactory(sessionFactory);
    return txManager;
  }


  @Bean
  public SendGrid sendGrid() {
    return new SendGrid(MAIL_API_KEY);
  }

  @Bean
  public ITemplateResolver htmlEmailSource() {
    final ClassLoaderTemplateResolver templateResolver = new ClassLoaderTemplateResolver();
    templateResolver.setTemplateMode(TemplateMode.HTML);
    templateResolver.setOrder(1);
    templateResolver.setPrefix("/emails/");
    templateResolver.setSuffix(".html");
    templateResolver.setCacheable(false);
    return templateResolver;
  }

  @Bean
  public TemplateEngine templateEngine() {
    final SpringTemplateEngine templateEngine = new SpringTemplateEngine();
    templateEngine.addTemplateResolver(htmlEmailSource());
    return templateEngine;
  }

  @Bean
  public PersistenceExceptionTranslationPostProcessor exceptionTranslation() {
    return new PersistenceExceptionTranslationPostProcessor();
  }

  Properties hibernateProperties() {
    return new Properties() {
      {
        //                setProperty(AvailableSettings.SHOW_SQL, "true");
        setProperty("hibernate.search.analyzer", "com.dataparse.server.util.hibernate.search.analyzer.StandardAnalyzer");
        setProperty("hibernate.search.default.directory_provider", "org.hibernate.search.store.impl.FSDirectoryProvider");
        setProperty("hibernate.search.default.indexBase", "./lucene/indexes");
        setProperty(AvailableSettings.HBM2DDL_AUTO, getDbRecreate() ? "create-drop" : "none");
        setProperty(AvailableSettings.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect");
        setProperty(AvailableSettings.GLOBALLY_QUOTED_IDENTIFIERS, "true");
      }
    };
  }


  @Bean
  public Docket dataParseApi() {
    return new Docket(DocumentationType.SWAGGER_2)
        .select()
        .apis(RequestHandlerSelectors.any())
        .paths(PathSelectors.any())
        .build()
        .pathMapping("/")
        .directModelSubstitute(LocalDate.class,
            String.class)
        .genericModelSubstitutes(ResponseEntity.class)
        .alternateTypeRules(
            newRule(typeResolver.resolve(DeferredResult.class,
                typeResolver.resolve(ResponseEntity.class, WildcardType.class)),
                typeResolver.resolve(WildcardType.class)))
        .useDefaultResponseMessages(false)
        .globalResponseMessage(RequestMethod.GET,
            newArrayList(new ResponseMessageBuilder()
                .code(500)
                .message("500 message")
                .responseModel(new ModelRef("Error"))
                .build()))
        .securitySchemes(newArrayList(apiKey()))
        .securityContexts(newArrayList(securityContext()))
        .enableUrlTemplating(false)
        ;
  }

  @Autowired
  private TypeResolver typeResolver;

  private ApiKey apiKey() {
    return new ApiKey("mykey", "api_key", "header");
  }

  private SecurityContext securityContext() {
    return SecurityContext.builder()
        .securityReferences(defaultAuth())
        .forPaths(PathSelectors.regex("/anyPath.*"))
        .build();
  }

  List<SecurityReference> defaultAuth() {
    AuthorizationScope authorizationScope
    = new AuthorizationScope("global", "accessEverything");
    AuthorizationScope[] authorizationScopes = new AuthorizationScope[1];
    authorizationScopes[0] = authorizationScope;
    return newArrayList(
        new SecurityReference("mykey", authorizationScopes));
  }

  @Bean
  SecurityConfiguration security() {
    return new SecurityConfiguration(
        "test-app-client-id",
        "test-app-realm",
        "test-app",
        "apiKey");
  }

  @Bean
  UiConfiguration uiConfig() {
    return new UiConfiguration(
        "validatorUrl");
  }
}