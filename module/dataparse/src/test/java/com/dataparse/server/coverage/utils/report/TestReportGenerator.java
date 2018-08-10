package com.dataparse.server.coverage.utils.report;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring4.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


@Slf4j
@Data
@Service
public class TestReportGenerator {

    // report generation data\configuration
    private TemplateEngine templateEngine;

    final private String templateName = "generalCoverageReport";
    final private String templatesLocation = "/coverage/templates/";
    final private String reportName = "testResult.html";
    final private String reportLocation = "src/main/resources/assets/";

    // creating template engine
    public TestReportGenerator() {
        ClassLoaderTemplateResolver templateResolver = new ClassLoaderTemplateResolver();

        templateResolver.setTemplateMode(TemplateMode.HTML);
        templateResolver.setOrder(1);
        templateResolver.setPrefix(templatesLocation);
        templateResolver.setSuffix(".html");
        templateResolver.setCacheable(false);

        templateEngine = new SpringTemplateEngine();
        templateEngine.addTemplateResolver(templateResolver);
    }

    // =================================================================================================================

    // generating report template as string
    public void generateReport(ReportData reportData) {
        Context templateContext = getFilledTemplateContext(reportData);
        String reportContent = templateEngine.process(templateName, templateContext);

        // @for debug
        // log.info("Generated report for tests: \n {}",reportContent);

        try {
            saveReportFile(reportContent);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }


    // =================================================================================================================

    // saving rendered report file
    private void saveReportFile(String reportContent) throws IOException {
        Path reportFile = Paths.get(reportLocation + reportName);

        Files.deleteIfExists(reportFile);
        Files.createFile(reportFile);

        BufferedWriter writer = Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8, StandardOpenOption.WRITE);
        writer.write(reportContent);
        writer.close();
    }

    // filling template context with report data
    private Context getFilledTemplateContext(ReportData reportData) {
        Context context = new Context();

        context.setVariable("reportCreated", new java.util.Date());
        context.setVariable("testKeys", reportData.getTestsKeys());
        context.setVariable("testResults", reportData.getTestsData());
        context.setVariable("testStatistics", reportData.getReportStatistics());

        return context;
    }

}
