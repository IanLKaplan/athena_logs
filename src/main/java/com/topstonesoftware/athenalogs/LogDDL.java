package com.topstonesoftware.athenalogs;

public class LogDDL {

    private LogDDL() {}

    private static String quoteStr(String str) {
        return "`" + str + "`";
    }

    public static String getLogTableDDL(String database, String tableName, String s3OrcFilePath) {
        final String formatStr = "%s%s %s,%n";
        final String stringType = "string";
        final String intType = "int";
        final String timestampType = "timestamp";
        String spacing = String.format("%4s", " ");
        return "create external table " +
                quoteStr(database + "." + tableName) +
                " (\n" +
                String.format(formatStr, spacing, quoteStr("bucket_name"), stringType) +
                String.format(formatStr, spacing, quoteStr("request_date"), timestampType) +
                String.format(formatStr, spacing, quoteStr("remote_ip"), stringType) +
                String.format(formatStr, spacing, quoteStr("operation"), stringType) +
                String.format(formatStr, spacing, quoteStr("key"), stringType) +
                String.format(formatStr, spacing, quoteStr("request_uri"), stringType) +
                String.format(formatStr, spacing, quoteStr("http_status"), intType) +
                String.format(formatStr, spacing, quoteStr("total_time"), intType) +
                String.format(formatStr, spacing, quoteStr("referrer"), stringType) +
                String.format(formatStr, spacing, quoteStr("user_agent"), stringType) +
                String.format(formatStr, spacing, quoteStr("version_id"), stringType) +
                String.format("%s%s %s%n", spacing, quoteStr("end_point"), stringType) +
                " )\n" +
                // "partitioned by (domain string)\n" +
                "stored as ORC\n" +
                "location 's3://" + s3OrcFilePath + "'\n" +
                "tblproperties (\"orc.compress\"=\"ZLIB\")";
    }

}
