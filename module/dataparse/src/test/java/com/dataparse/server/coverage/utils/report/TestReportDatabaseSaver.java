package com.dataparse.server.coverage.utils.report;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.parser.jdbc.Protocol;
import com.dataparse.server.util.DbUtils;
import com.dataparse.server.util.SystemUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.List;
import java.util.Map;


@Slf4j
public class TestReportDatabaseSaver {

    // db connection data
    private Protocol DB_PROTOCOL = Protocol.valueOf(System.getProperty("DB_PROTOCOL", Protocol.postgresql.name()));
    private String TESTRESULT_DB_NAME = SystemUtils.getProperty("TESTRESULT_DB_NAME", "dataparse_test");
    private String TESTRESULT_DB_HOST = SystemUtils.getProperty("TESTRESULT_DB_HOST", "localhost");
    private Integer DB_PORT = Integer.parseInt(System.getProperty("DB_PORT", "5432"));
    private String DB_USER = System.getProperty(AppConfig.DB_USERNAME, "testuser");
    private String DB_PASSWORD = System.getProperty(AppConfig.DB_PASSWORD, "testuser");
    private String COMMIT_HASH = System.getenv("CI_COMMIT_SHA");
    private String TEST_REPORT_TABLE = "test_reports";

    // saving report data
    final private String SESSION = System.getProperty("SESSION");

    public void saveReportData(ReportData reportData) {
        try (Connection connection = getDBConnection()) {
            log.info("Started saving report data to database.");

            createReportsTableIfNotExists(connection);
            log.info("Ensured table {} existence.", TEST_REPORT_TABLE);

            insertReportData(connection, reportData);
            log.info("Inserted report data to database.");

        } catch (SQLException e) {
            log.error("Failed during saving test data to database.");
            e.printStackTrace();
        }
    }

    // =================================================================================================================
    // Saver utils
    // =================================================================================================================

    private Connection getDBConnection() {
        ConnectionParams params = new ConnectionParams();
        params.setProtocol(DB_PROTOCOL);
        params.setDbName(TESTRESULT_DB_NAME);
        params.setHost(TESTRESULT_DB_HOST);
        params.setPort(DB_PORT);
        params.setUser(DB_USER);
        params.setPassword(DB_PASSWORD);
        return DbUtils.createConnection(params);
    }

    private void createReportsTableIfNotExists(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS test_reports (\n" +
                "  uuid           CHAR(64),\n" +
                "  test_date      TIMESTAMP ,\n" +
                "  step_duration INTEGER,\n" +
                "  step_succeeded BOOL,\n" +
                "  test_name      CHAR(64),\n" +
                "  step_number    INTEGER,\n" +
                "  step_name      CHAR(128),\n" +
                "  step_status    TEXT,\n" +
                "  test_params    CHAR(128),\n" +
                "  test_file_name    CHAR(64),\n" +
                "  test_file_ext    CHAR(8),\n" +
                "  test_backend    CHAR(32),\n" +
                "  commit_hash    CHAR(128)\n" +
                ") ");
    }

    private void insertReportData(Connection connection, ReportData reportData) {
        String query = String.format("INSERT INTO %s" +
                "(uuid, test_date, step_duration, step_succeeded, test_name, " +
                "step_number, step_name, step_status, test_params, " +
                "test_file_name, test_file_ext, test_backend, " +
                "commit_hash) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", TEST_REPORT_TABLE);

        try (PreparedStatement statement = connection.prepareStatement(query)) {

            for (Map.Entry<String, TestResult> entry : reportData.getTestsData().entrySet()) {
                TestResult test = entry.getValue();
                List<SubTestResult> steps = test.getSubTests();

                int stepIdx = 1;
                for (SubTestResult step : steps) {

                    statement.setString(1, SESSION);
                    statement.setTimestamp(2, new Timestamp(reportData.getReportDateTime().getMillis()));
                    statement.setLong(3, step.getDuration());
                    statement.setBoolean(4, step.getSucceeded());
                    statement.setString(5, step.getTestName());
                    statement.setInt(6, stepIdx);
                    statement.setString(7, step.getName());
                    statement.setString(8, step.getDescription());
                    statement.setString(9, step.getTestParams());
                    statement.setString(10, step.getTestedFileName());
                    statement.setString(11, step.getTestedFileExt());
                    statement.setString(12, step.getTestBackendEngine());
                    statement.setString(13, COMMIT_HASH == null ? "LOCAL" : COMMIT_HASH);

                    statement.addBatch();
                    stepIdx++;
                }
            }

            statement.executeBatch();

        } catch (SQLException e) {
            log.error("Report data wasn't inserted correctly.");

            e.printStackTrace(); // general reason
            e.getNextException().printStackTrace(); // specific error cause
        }
    }

}
