package com.topstonesoftware.athenalogs;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;


/**
 * <h3>
 *     Environment setup (for bash):
 * </h3>
 * <pre>
 *     AWS_REGION="us-west-1"
 *     AWS_ATHENA_KEY_ID="my key id for Athena user"
 *     AWS_ATHENA_ACCESS_KEY="my secret key for Athena user"
 *
 *     export AWS_REGION
 *     export AWS_ATHENA_KEY_ID
 *     export AWS_ATHENA_ACCESS_KEY
 * </pre>
 *
 * <h3>
 *     Command line:
 * </h3>
 * <p>
 *     Example:
 * </p>
 * <pre>
 *     --orcPath ianlkaplan-logs.orc/user/iank/http_logs --dbName orclogdb --tableName httplogs --domain bearcave.com
 * </pre>
 */
@Slf4j
@Builder
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final String ORC_PATH = "orcPath";
    private static final String DB_NAME = "dbName";
    private static final String TABLE_NAME = "tableName";
    private static final String DOMAIN_NAME = "domain";
    private static final String HELP = "help";

    @NonNull
    String orcPath;
    @NonNull
    String dbName;
    @NonNull
    String tableName;
    @NonNull
    String domainName;

    private void listDomains(List<String> domains) {
        System.out.println("Domains:");
        if (domains.isEmpty()) {
            System.out.println("No domains found");
        } else {
            for (String domain : domains) {
                System.out.println(domain);
            }
        }
    }

    private void printTree(DirTreeNode root, int level) {
        final int spaces = 4;
        int padding = spaces + spaces * level;
        String formatStr = "%" + padding + "s%s %d%n";
        System.out.format(formatStr, "", root.getDirName(), root.getPageRefCnt());
        List<DirTreeNode> children = root.getChildren();
        if (! children.isEmpty()) {
            for (DirTreeNode node : children) {
                printTree(node, level + 1);
            }
        }
    }

    /**
     * Print a directory tree built from S3 paths
     */
    private void printTree(DirTreeNode root) {
        printTree(root, 0);
    }

    private void runQueries() {
        AthenaDB athenaDB = new AthenaDB();
        try {
            Optional<Connection> optConn = athenaDB.getAthenaConnection();
            if (optConn.isPresent()) {
                try (Connection conn = optConn.get()) {
                    AthenaDB.buildDatabaseAndTable(conn, dbName, tableName, orcPath);
                    LogDataFacets dataFacets = new LogDataFacets(conn, dbName, tableName);
                    List<String> domainList = dataFacets.getEndPoints();
                    listDomains(domainList);
                    List<Pair<String, Integer>> pathPairs = dataFacets.getPaths(conn, dbName, tableName, domainName);
                    DirTreeNode root = dataFacets.buildDirTree(pathPairs);
                    System.out.println("Path tree for " + domainName);
                    printTree(root);
                    List<Pair<String, Integer>> referrerPairs = dataFacets.getReferrers(conn, dbName, tableName, domainName);
                    System.out.println("Referrer sites:");
                    for (int i = 0; i < Math.min(10, referrerPairs.size()); i++) {
                        System.out.println(referrerPairs.get(i).getLeft() + ", " + referrerPairs.get(i).getRight());
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("runQueries: {}", e.getLocalizedMessage());
        }
    }

    private static Options buildOptions() {
        Options options = new Options();
        Option orcPath = Option.builder()
                .longOpt( ORC_PATH )
                .hasArg()
                .desc("The S3 bucket and path for the ORC files. Example: ianlkaplan-logs.orc/user/iank/http_logs Bucket is ianlkaplan-logs.orc")
                .required()
                .build();
        options.addOption( orcPath);
        Option dbName = Option.builder()
                .longOpt( DB_NAME )
                .hasArg()
                .desc("The database name")
                .required()
                .build();
        options.addOption(dbName);
        Option dbTableName = Option.builder()
                .longOpt( TABLE_NAME )
                .hasArg()
                .desc("The database table name")
                .required()
                .build();
        options.addOption(dbTableName);
        Option domainName = Option.builder()
                .longOpt( DOMAIN_NAME )
                .hasArg()
                .desc("The domain name to be selected (e.g., example.com)")
                .required()
                .build();
        options.addOption(domainName);
        Option helpOpt = Option.builder()
                .longOpt( HELP )
                .hasArg(false)
                .desc("Display the command line options")
                .required(false)
                .build();
        options.addOption(helpOpt);
        return options;
    }

    public static void help(Options cliOptions) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "athena_logs", cliOptions );
    }

    public static void main(String[] args) {
        Options cliOptions = buildOptions();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(cliOptions, args);
            if (commandLine.hasOption("help")) {
                help(cliOptions);
            } else {
                String orcPathArg = commandLine.getOptionValue( ORC_PATH );
                String dbNameArg = commandLine.getOptionValue( DB_NAME );
                String tableNameArg = commandLine.getOptionValue( TABLE_NAME );
                String domainNameArg = commandLine.getOptionValue( DOMAIN_NAME );
                Main main = Main.builder()
                        .orcPath(orcPathArg)
                        .dbName(dbNameArg)
                        .tableName(tableNameArg)
                        .domainName( domainNameArg)
                        .build();
                main.runQueries();
            }

        } catch (ParseException e) {
            logger.error("Error parsing command line arguments: {}", e.getLocalizedMessage());
            help(cliOptions);
        }
    }
}
