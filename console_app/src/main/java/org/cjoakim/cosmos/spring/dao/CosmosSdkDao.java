package org.cjoakim.cosmos.spring.dao;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.model.TelemetryEvent;
import org.cjoakim.cosmos.spring.model.TelemetryQueryResults;

import java.util.ArrayList;

/**
 * This is a Data Access Object (DAO) which uses the CosmosDB SDK for Java
 * rather than Spring Data.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Slf4j
public class CosmosSdkDao {

    private CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;

    private String uri;
    private String key;
    private String currentDbName;
    private String currentContainerName = "";

    boolean verbose;

    public CosmosSdkDao() {

        super();
    }

    public CosmosClient initialize(String uri, String key, String dbName, boolean verbose) {

        this.uri = uri;
        this.key = key;
        this.currentDbName = dbName;

        if (verbose) {
            log.warn("uri:    " + uri);
            log.warn("key:    " + key);
            log.warn("dbName: " + dbName);
        }

        if (client == null) {
            client = new CosmosClientBuilder()
                    .endpoint(uri)
                    .key(key)
                    .buildClient();
            log.warn("client: " + client);

            database = client.getDatabase(this.currentDbName);
            log.warn("client connected to database Id: " + database.getId());
        }
        return client;
    }

    public void close() {

        if (client != null) {
            log.warn("closing...");
            client.close();
            log.warn("closed");
        }
    }

    public void setCurrentContainer(String c) {

        if (this.currentContainerName.equalsIgnoreCase(c)) {
            return;
        }
        else {
            container = database.getContainer(c);
            this.currentContainerName = c;
        }
    }

    public TelemetryQueryResults getAllTelemetry() {

        String sql = "select * from c";
        int    pageSize = 100;
        String continuationToken = null;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

        TelemetryQueryResults resultsStruct = new TelemetryQueryResults();
        resultsStruct.setSql(sql);

        // Execute the SQL query and iterate the paginated result set,
        // collecting the documents and total RU charge.
        do {
            Iterable<FeedResponse<TelemetryEvent>> feedResponseIterator =
                    container.queryItems(sql, queryOptions, TelemetryEvent.class)
                            .iterableByPage(continuationToken, pageSize);

            for (FeedResponse<TelemetryEvent> page : feedResponseIterator) {
                for (TelemetryEvent doc : page.getResults()) {
                    resultsStruct.addDocument(doc);
                }
                resultsStruct.addRequestCharge(page.getRequestCharge());
                resultsStruct.incrementPageCount();
                continuationToken = page.getContinuationToken();
            }
        }
        while (continuationToken != null);

        return resultsStruct;
    }
}
