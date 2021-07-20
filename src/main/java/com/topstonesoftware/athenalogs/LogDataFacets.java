package com.topstonesoftware.athenalogs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * This class queries the Athena web log database for various types of information. This code assumes that the
 * queries run over ORC files constructed from AWS S3 web log data. See https://github.com/IanLKaplan/s3logreader
 * The class assumes that the database has a single table for the log data. This table may include S3 web log
 * data for more than one web domain.
 */
@Slf4j
public class LogDataFacets {
    private static final Logger logger = LoggerFactory.getLogger(LogDataFacets.class);
    private final Connection conn;
    private final String database;
    private final String logTable;

    public LogDataFacets(Connection conn, String database, String logTable) {
        this.conn = conn;
        this.database = database;
        this.logTable = logTable;
    }

    /**
     * Get the s3 endpoints which correspond to the domain names (e.g., bearcave.com)
     *
     * @return a list of end point names.
     */
    public List<String> getEndPoints() {
        final String query = "select distinct bucket_name as domain from " + database + "." + logTable;
        List<String> domainList = new ArrayList<>();
        try {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet result = stmt.executeQuery(query)) {
                    while (result.next()) {
                        String domain = result.getString("domain");
                        if (domain != null) {
                            domainList.add(domain);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("getEndPoints: {}", e.getLocalizedMessage());
        }
        return domainList;
    }


    /**
     *  Get the HTML and HTM page paths for a given domain (a.k.a. S3 file keys) and the count.
     *  For example:
     *
     *  <pre>
     *      path, count
     *      misl/misl_tech/wavelets/hurst/index.html, 215
     *      software/java/xml/xmlpull_license.html, 207
     *      links.htm, 188
     *      software/divide.htm, 161
     *      misl/misl_tech/signal/idft/index.html, 137
     *      bookrev/neuromancer/neuromancer_audio.html, 133
     *      misl/misl_other/nigerian_scam.html, 127
     *      finance/wrds_data.html, 125
     *  </pre>
     *  <p>
     *      The query is limited to the HTTP 200 return status to avoid returning pages non-existent pages, especially
     *      from sites that are hunting for vulnerabilities. For example '/backup/bitcoin.html' or
     *      'mysql_dumps/mysql_dumps/index.html'
     *  </p>
     *
     * @return a list of page paths
     */
    public List<Pair<String, Integer>> getPaths(Connection conn, String database, String table, String domain) {
        List<Pair<String, Integer>> pathList = new ArrayList<>();
        String dbTable = database + "." + table;
        String query = "select key as path, count(key) as count from " + dbTable + "\n" +
                       "where bucket_name = ? and http_status = 200 and (key like '%.html' or key like '%.htm')\n" +
                       "group by key\n" +
                       "order by count desc";
        try {
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, domain);
                try (ResultSet rslt = stmt.executeQuery()) {
                    while (rslt.next()) {
                        String path = rslt.getString("path");
                        Integer count = rslt.getInt("count");
                        Pair<String, Integer> pair = new ImmutablePair<>(path, count);
                        pathList.add(pair);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("getPaths: {}", e.getLocalizedMessage());
        }
        return pathList;
    }

    /**
     * The web page reference counts initially exist at the leaves of the directory tree.
     * Propagate the counts upward and sum them for each root in the tree.
     *
     * @param root The current tree root.
     * @return the reference coujnt for that node/root
     */
    public Integer fillInRefCounts(DirTreeNode root) {
        List<DirTreeNode> children = root.getChildren();
        Integer sum = 0;
        if (! children.isEmpty()) {
            for (DirTreeNode node : children) {
                Integer cnt = fillInRefCounts( node );
                sum += cnt;
            }
            root.setPageRefCnt( sum );
        } else {
            sum = root.getPageRefCnt();
        }
        return sum;
    }

    /**
     *
     * @param pathList a list of String, Integer pairs, where the String is an S3 path key.
     * @return A tree built from the S3 path list
     */
    public DirTreeNode buildDirTree(@NotNull List<Pair<String, Integer>> pathList) {
        DirTreeNode rootNode = new DirTreeNode("/");
        for (Pair<String, Integer> pathInfo : pathList) {
            rootNode.addPath(pathInfo);
        }
        // Fill in the page reference counts
        fillInRefCounts(rootNode);
        return rootNode;
    }

    /**
     * <p>
     *     Return a list of domains that refer to the pages in this domain (e.g., referrer sites). These will generally
     *     be google or other search engines or web pages with a link to the domain pages.
     * </p>
     * <p>
     *     An example of the raw output from the query is shown below:
     * </p>
     * <pre>
     * -,45575
     * google.com/,25709
     * google.com/,2559
     * google.co.uk/,1996
     * duckduckgo.com/,1238
     * google.ca/,769
     * bing.com/,758
     * google.com.au/,450
     * google.de/,241
     * </pre>
     * <p>
     *     In this output each of the strings is enclosed by a double quote.
     * </p>
     * <p>
     *  Some notes on the SQL:
     * </p>
     * <ul>
     *     <li>An attempt is made to filter out Amazon internal references by filtering out the Amazon IP string 52.219</li>
     *     <li>An attempt is made to filter out search strings</li>
     *     <li>References within the domain are filtered out (e.g., one domain page referring to another.</li>
     *     <li>There is a reference "-" (quotes in the string) which is a bad domain reference marker. This is filtered out.</li>
     * </ul>
     * @param conn the database connectoin
     * @param database the database name
     * @param table the log table name
     * @param domain the domain to filter the results on
     * @return a list of referrer domain/path names and counts
     */
    public List<Pair<String, Integer>> getReferrers(Connection conn, String database, String table, String domain) {
        String dbTableName = database + "." + table;
        String query = """
        select replace(replace(replace(replace(referrer, 'http://', ''), 'https://', ''), 'www.', ''), '"', '') as referrer, count(referrer) as count from DBTABLE 
        where bucket_name = ? and http_status = 200 and
        referrer not like '%52.219.%' and referrer not like '%search%'
        and referrer not like ?
        and (key like '%.html' or key like '%.htm')
        group by referrer
        order by count desc
        """;
        query = query.replace("DBTABLE", dbTableName);
        Map<String, Integer> pairMap = new HashMap<>();
        List<Pair<String, Integer>> pairList = new ArrayList<>();
        try {
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, domain);
                stmt.setString(2, "%" + domain + "%");
                try (ResultSet rslt = stmt.executeQuery()) {
                    while (rslt.next()) {
                        String ref = rslt.getString("referrer");
                        if (! ref.equals("-")) {
                            Integer count = rslt.getInt("count");
                            if (pairMap.containsKey(ref)) {
                                Integer existingCount = pairMap.get(ref);
                                pairMap.put(ref, existingCount + count);
                            } else {
                                pairMap.put(ref, count);
                            }
                        }
                    }
                }
            }
            if (pairMap.size() > 0) {
                Set<Map.Entry<String, Integer>> mapSet = pairMap.entrySet();
                for (Map.Entry<String, Integer> val : mapSet) {
                    Pair<String, Integer> pair = new ImmutablePair<>(val.getKey(), val.getValue());
                    pairList.add( pair );
                }
            }
        } catch (SQLException e) {
            logger.error("getPaths: {}", e.getLocalizedMessage());
        }
        // Sort the result in descending order
        pairList.sort(Collections.reverseOrder(Comparator.comparing(Pair::getRight)));
        return pairList;
    }  // getReferrers

}
