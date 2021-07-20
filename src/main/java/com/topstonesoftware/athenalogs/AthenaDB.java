package com.topstonesoftware.athenalogs;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class AthenaDB {
    private static final Logger logger = LoggerFactory.getLogger(AthenaDB.class);
    private static final String ATHENA_BUCKET = "s3://ianlkaplan-athena-scratch";
    private static final String ID = "AWS_ATHENA_KEY_ID";
    private static final String KEY = "AWS_ATHENA_ACCESS_KEY";
    private static final String REGION = "AWS_REGION";

    private Properties buildProperties() {
        Properties properties = new Properties();
        String id = System.getenv( ID );
        String key = System.getenv(KEY);

        if (id != null && (!id.isEmpty()) && key != null && (!key.isEmpty())) {
            properties.put("User", id);
            properties.put("Password", key);
            properties.put("S3OutputLocation", ATHENA_BUCKET);
            properties.put("AwsDataCatalog", "AwsDataCatalog");
        }
        return properties;
    }

    public Optional<Connection> getAthenaConnection() throws SQLException {
        Optional<Connection> optConnection = Optional.empty();
        Properties properties = buildProperties();
        if (properties.size() > 0) {
            String regionStr = System.getenv(REGION);
            Connection conn = DriverManager.getConnection("jdbc:awsathena://AwsRegion=" + regionStr, properties);
            optConnection = Optional.of(conn);
        } else {
            logger.error("Error in properties");
        }
        return optConnection;
    }

    public static int executeUpdate(Connection conn, String sql) throws SQLException {
        try(Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    public static boolean execute(Connection conn, String sql) throws SQLException {
        try(Statement stmt = conn.createStatement()) {
            return stmt.execute(sql);
        }
    }


    public static boolean hasTable(Connection conn, String tableName) throws SQLException {
        final String query = "show tables in orclogdb";
        boolean foundTable = false;
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet result = stmt.executeQuery(query)) {
                while (result.next()) {
                    String table = result.getString("tab_name");
                    if (table != null && table.length() > 0 && table.equals(tableName)) {
                        foundTable = true;
                        break;
                    }
                }
            }
        }
        return foundTable;
    }


    public static void buildDatabaseAndTable(Connection conn, String database, String logTable, String orcFilePath) throws SQLException {
        final String createDB = "create database if not exists " + database;
        AthenaDB.executeUpdate(conn, createDB);
        if (! AthenaDB.hasTable(conn, logTable)) {
            String createTableQuery = LogDDL.getLogTableDDL(database, logTable, orcFilePath);
            AthenaDB.execute(conn, createTableQuery);
        }
    }

}
