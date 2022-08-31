package org.cjoakim.cosmos.spring.dao;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a Data Access Object (DAO) which uses the CosmosDB SDK for Java
 * rather than Spring Data.  This class isn't used in the web application,
 * it is just for ad-hoc and exploratory purposes.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Slf4j
public class CosmosDAO {

    private CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;

    private String uri;
    private String key;
    private String currentDbName;
    private String currentContainerName = "";

    public CosmosDAO() {

        super();
    }

    public CosmosClient initialize(String uri, String key, String dbName) {

        this.uri = uri;
        this.key = key;
        this.currentDbName = dbName;

        if (client == null) {
            log.warn("getClient, uri: " + uri + " key: " + key + " currentDbName: " + currentDbName);

            client = new CosmosClientBuilder()
                    .endpoint(uri)
                    .key(key)
                    .buildClient();
            log.warn("client: " + client);

            database = client.getDatabase(this.currentDbName);
            log.warn("database Id: " + database.getId());
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

    private void setCurrentContainer(String c) {

        if (this.currentContainerName.equalsIgnoreCase(c)) {
            return;
        }
        else {
            container = database.getContainer(c);
            this.currentContainerName = c;
        }
    }

}
