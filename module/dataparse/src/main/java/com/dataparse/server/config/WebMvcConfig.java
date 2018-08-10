package com.dataparse.server.config;

import com.dataparse.server.auth.AuthInterceptor;
import com.dataparse.server.auth.SessionInterceptor;
import com.dataparse.server.util.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

@Configuration
@ComponentScan(basePackages = "com.dataparse.server")
@EnableWebMvc
public class WebMvcConfig extends WebMvcConfigurerAdapter {
  @Autowired
  private Environment environment;

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    StringHttpMessageConverter stringConverter = new StringHttpMessageConverter(Charset.forName("UTF-8"));
    List<MediaType> mediaTypes = Arrays.asList( MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON);
    stringConverter.setSupportedMediaTypes(mediaTypes);
    converters.add(stringConverter);
    converters.add(new MappingJackson2HttpMessageConverter(JsonUtils.mapper));
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
//    String projectDir = environment.getProperty("PROJECT_DIR");
//    String assets = projectDir == null ? "classpath:assets/" : "file:" + projectDir + "src/main/resources/assets/";
//    registry.
//      addResourceHandler("/**").
//      addResourceLocations("classpath:assets/");

    
    String webappLoc = System.getProperty("app.webapp.dir");
    if(webappLoc == null) { 
      String appHome = System.getProperty("app.home");
      if(appHome.startsWith("/")) {
        webappLoc = "file:" + appHome + "/webapp/";
      } else {
        webappLoc = "file:/" + appHome + "/webapp/";
      }
    }
    
    registry.
      addResourceHandler("/static/**").
      addResourceLocations(webappLoc);
    
    registry.
      addResourceHandler("swagger-ui.html").
      addResourceLocations("classpath:/META-INF/resources/");
    
    registry.
      addResourceHandler("/webjars/**").
      addResourceLocations("classpath:/META-INF/resources/webjars/");
/**    
    registry.
      addResourceHandler("/css/**").
      addResourceLocations(assets + "css/");
    
    registry.
      addResourceHandler("/images/**").
      addResourceLocations(assets + "images/");
*/
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new SessionInterceptor());
    registry.addInterceptor(new AuthInterceptor())
    .addPathPatterns("/**")
    .excludePathPatterns(
        "/",
        "/404",
        //                        "/datadocs.com.html",
        "/embed/**",
        "/api/export/**",
        "/api/visualization/**", // todo should have a separate route or add authorizations instead
        "/api/user/upload_avatar",
        "/api/auth/**",
        "/api/user/forgot-password",
        "/api/user/reset-password",
        "/api/user/register",
        "/static/**");
  }
}
