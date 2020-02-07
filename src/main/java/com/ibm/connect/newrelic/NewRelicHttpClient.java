package com.ibm.connect.newrelic;

import static com.ibm.connect.newrelic.NewRelicSourceConnectorConfig.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.*;

public class NewRelicHttpClient {

    static final Logger log = LoggerFactory.getLogger(NewRelicHttpClient.class);
    private OkHttpClient _okHttpClient;

    private int MAX_RETRIES;
    private int RETRY_BACKOFF_MS;
    private final int UNBOUNDED_NUMBER_OF_RETRIES = -1;

    private NewRelicSourceConnectorConfig _config;

    public NewRelicHttpClient(NewRelicSourceConnectorConfig config) {
        this._config = config;
    }

    public void init() throws IOException {
        final int CONNECTION_TIMEOUT_SECONDS = _config.getInt(HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS);//10
        final int CALL_TIMEOUT_SECONDS = _config.getInt(HTTP_CLIENT_TIMEOUT_SECONDS);//30 // Disabled by default. Read/Write timeouts will be respected.
        final int READ_TIMEOUT_SECONDS = _config.getInt(HTTP_CLIENT_READ_TIMEOUT_SECONDS);//25
        final int MAX_IDLE_CONNECTIONS = _config.getInt(HTTP_CLIENT_MAX_IDLE_CONNECTIONS);//2
        final int KEEP_ALIVE_DURATION_SECONDS = _config.getInt(HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS);//30

        this._okHttpClient = new OkHttpClient.Builder()
                                             .connectionPool(new ConnectionPool(MAX_IDLE_CONNECTIONS, KEEP_ALIVE_DURATION_SECONDS, TimeUnit.SECONDS))
                                             .connectTimeout(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                             .callTimeout(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                             .readTimeout(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                             .build();

        this.MAX_RETRIES = _config.getInt(HTTP_CLIENT_MAX_RETRIES);//-1
        this.RETRY_BACKOFF_MS = _config.getInt(HTTP_CLIENT_RETRY_BACKOFF_SECONDS) * 1000;//30
    }

    public void close() {
        try {
            this._okHttpClient.connectionPool().evictAll();
        } catch(Exception ex) {
            log.debug("Error while cleaning up connection pool. [{}]", ex);
        }
    }

    private String getSinceClause(long timestamp) throws IOException {
        long currEpochMilli = Instant.now().toEpochMilli();
        log.info("currts:{} ints:{}", currEpochMilli, timestamp);

        if(currEpochMilli < timestamp) {
            throw new ConnectException("Cannot get data from a future timestamp");
        }
        long diff = currEpochMilli - timestamp;
        long days = diff / 86400000;

        String clause = "SINCE ";
        if(days > 0) {
            return clause + days + " DAYS AGO";
        }

        long hours = diff / 3600000;
        if(hours > 0) {
            return clause + hours + " HOURS AGO";
        }

        long mins = diff / 60000;

        return clause + (mins > 0 ? mins : 1) + " MINUTES AGO";
    }

    public List<JSONObject> getRecords(Long fromTimestamp) throws InterruptedException, IOException {
        Request.Builder req = new Request.Builder()
                                         .url(getRequestUrl(fromTimestamp).toString())
                                         .get();

        Response response = this.call(req);
        JSONObject result = null;
        ResponseBody body = null;
        try {
            body = response.body();
            if (body != null) {
                result = new JSONObject(body.string());
            }
        } finally {
            if (body != null) {
                body.close();
            }
        }

        List<JSONObject> results = new ArrayList<JSONObject>();
        if (result != null) {
            JSONArray records = result.optJSONArray("results").optJSONObject(0).optJSONArray("events");
            if (records != null) {
                for (int i = 0; i < records.length(); i++) {
                    results.add(records.getJSONObject(i));
                    log.debug("recd: [{}]", results.get(i).toString());
                }
            } else {
                log.error("Received the following body that could not be parsed. [{}]", result);
            }
        } else {
            log.error("Some other query error");
        }

        log.debug("-> query execution result [{}]", results == null ? "NULL" : results.size() + " records");
        return results;
    }

    private StringBuilder getRequestUrl(Long fromTimestamp) throws IOException {
        log.debug("-::::::::::::::::::::::::::::- getRequestUrl ");

        final String baseUrl = _config.getBaseUrl(); 
        final String accountId = _config.getAccount(); 
        final String queryTable = _config.getNrqlQueryTable();
        log.debug("-> queryTable: [{}]", queryTable) ;
        final String queryTimestampColumn = _config.getNrqlQueryTimestampColumn();
        log.debug("-> queryTimestampColumn: [{}]", queryTimestampColumn) ;
        final String queryMetadataColumns = _config.getNrqlQueryMetadataColumns();
        log.debug("-> queryMetadataColumns: [{}]", queryMetadataColumns) ;
        final String queryValueColumns = _config.getNrqlQueryValueColumns(); 
        log.debug("-> queryValueColumns: [{}]", queryValueColumns) ;
        final int queryBatchSize = _config.getNrqlQueryBatchSize();
        log.debug("-> queryBatchSize: [{}]", queryBatchSize) ;
        final String queryWhereClause = _config.getNrqlQueryWhereClause();
        log.debug("-> queryWhereClause: [{}]", queryWhereClause) ;

        final StringBuilder requestUrl = new StringBuilder(String.format(baseUrl, accountId)); 

        requestUrl.append("?"); // https://insights-api.newrelic.com/v1/accounts/1234/query?

        // main query
        String query = String.format("SELECT %s,%s,%s FROM %s WHERE %s > %s", 
                                        queryValueColumns, 
                                        queryTimestampColumn, 
                                        queryMetadataColumns,
                                        queryTable, 
                                        queryTimestampColumn, 
                                        fromTimestamp); 
        query = query.replaceAll(":", " as ");
        
        // optional WHERE filters
        if(!Utils.empty(queryWhereClause)) {
            query = query + String.format(" AND %s", queryWhereClause);
        }

        // common to all queries
        query = query + String.format(" %s ORDER BY %s ASC LIMIT %s",
                                        getSinceClause(fromTimestamp),
                                        queryTimestampColumn,
                                        queryBatchSize);

        log.info("-> {}: query: {}", _config.getMetricName(), query);

        try {
            requestUrl.append(String.format("%s=%s", "nrql", URLEncoder.encode(query, "UTF-8")));
        } catch (UnsupportedEncodingException ex) {
            throw new ConnectException(String.format("The following error occurred while trying to encode the following query string. [%s]", query), ex);
        }
        return requestUrl;
    }

    private Response call(Request.Builder requestBuilder) throws InterruptedException {

        final String AUTHORIZATION_HEADER = "X-Query-Key";
        final String ACCEPT_HEADER = "Accept";
        Response response = null;

        int remainingRetries = this.MAX_RETRIES;
        while(this.MAX_RETRIES == this.UNBOUNDED_NUMBER_OF_RETRIES || remainingRetries-- > 0) {

            Response candidate = null;
            Request request = requestBuilder.addHeader(AUTHORIZATION_HEADER, _config.getApiKey())
                                            .addHeader(ACCEPT_HEADER, "application/json")
                                            .build();

            try {
                candidate = this._okHttpClient.newCall(request).execute();
            } catch (IOException ex) {
                log.error("The following error occurred while sending request [{}]. [{}]", request, ex.toString());
            }

            if (candidate == null || !candidate.isSuccessful()) {
                if (candidate != null) {
                    ResponseBody body = candidate.body();
                    String bodyContents = "";
                    try {
                        if (body != null) {
                            bodyContents = body.string();
                            body.close();
                        }
                    } catch (Exception ignored) {
                        // ignored.
                    }

                    log.error("Request [{}] failed with code [{}] and the following body. [{}]", request, candidate.code(),
                            bodyContents);
                    if (candidate.code() == 401) {
                        log.info("Received 401 from server, indicates API Key is incorrect, exit");
                    }
                }
            } else {
                // Received a successful response.
                response = candidate;
                break;
            }

            log.info("Retrying in [{}] ms. [{}] retries remaining.", this.RETRY_BACKOFF_MS, remainingRetries);
            Thread.sleep(this.RETRY_BACKOFF_MS);
        }

        if (response == null) {
            throw new ConnectException("Failed while making request. Bailing.");
        }

        return response;
    }
}
