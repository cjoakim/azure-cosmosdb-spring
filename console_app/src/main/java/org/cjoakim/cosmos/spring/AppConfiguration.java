package org.cjoakim.cosmos.spring;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.spring.data.cosmos.config.AbstractCosmosConfiguration;
import com.azure.spring.data.cosmos.config.CosmosConfig;
import com.azure.spring.data.cosmos.repository.config.EnableCosmosRepositories;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.repository.ResponseDiagnosticsProcessorImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This class is a Spring Boot @Configuration class that also provides configuration
 * values for the Spring Data CosmosDB repositories.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Configuration
@EnableCosmosRepositories
@Slf4j
public class AppConfiguration extends AbstractCosmosConfiguration implements AppConstants {

    private static AppConfiguration singleton;

    public AppConfiguration() {
        super();
        log.warn("DataAppConfiguration default constructor (singleton)");
        singleton = this;
    }

    private static String[] commandLineArgs = null;

    // Generic methods:

    protected static void setCommandLineArgs(String[] args) {

        commandLineArgs = args;
        if (commandLineArgs == null) {
            log.warn("setCommandLineArgs; null");
        }
        else {
            log.warn("setCommandLineArgs; length: " + commandLineArgs.length);
            for (int i = 0; i < commandLineArgs.length; i++) {
                log.warn("setCommandLineArgs, idx: " + i + " -> " + commandLineArgs[i]);
            }
        }
    }

    public static String flagArg(String flagArg) {

        for (int i = 0; i < commandLineArgs.length; i++) {
            if (commandLineArgs[i].equalsIgnoreCase(flagArg)) {
                return commandLineArgs[i + 1];
            }
        }
        return null;
    }

    public static String flagArg(String flagArg, String defaultValue) {

        for (int i = 0; i < commandLineArgs.length; i++) {
            if (commandLineArgs[i].equalsIgnoreCase(flagArg)) {
                return commandLineArgs[i + 1];
            }
        }
        return defaultValue;
    }

    public static boolean booleanArg(String flagArg) {

        for (int i = 0; i < commandLineArgs.length; i++) {
            if (commandLineArgs[i].equalsIgnoreCase(flagArg)) {
                return true;
            }
        }
        return false;
    }

    public static long longFlagArg(String flagArg, long defaultValue) {

        try {
            return Long.parseLong(flagArg(flagArg));
        }
        catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static boolean isVerbose() {

        return booleanArg(VERBOSE_FLAG);
    }

    public static boolean isSilent() {

        return booleanArg(SILENT_FLAG);
    }

    public static boolean isPretty() {

        return booleanArg(PRETTY_FLAG);
    }

    // Application Config:


    public static String getCosmosContainerName() {

        return flagArg(CONTAINER_FLAG, TELEMETRY_CONTAINER_NAME);
    }

    // CosmosDB Spring Data Config below:
    // See https://docs.microsoft.com/en-us/azure/developer/java/spring-framework/how-to-guides-spring-data-cosmosdb

    @Value("${spring.cloud.azure.cosmos.endpoint}")
    public String uri;

    //@Value("${azure.cosmos.key}")
    @Value("${spring.cloud.azure.cosmos.key}")
    public String key;

    //@Value("${azure.cosmos.database}")
    @Value("${spring.cloud.azure.cosmos.database}")
    private String dbName;

    @Value("${azure.cosmos.queryMetricsEnabled}")
    private boolean queryMetricsEnabled;

    @Value("${azure.cosmos.maxDegreeOfParallelism}")
    private int maxDegreeOfParallelism;

    private AzureKeyCredential azureKeyCredential;

    @Bean
    public CosmosClientBuilder getCosmosClientBuilder() {

        log.warn("getCosmosClientBuilder, uri: " + uri);

        this.azureKeyCredential = new AzureKeyCredential(key);
        DirectConnectionConfig directConnectionConfig = new DirectConnectionConfig();
        GatewayConnectionConfig gatewayConnectionConfig = new GatewayConnectionConfig();
        return new CosmosClientBuilder()
                .endpoint(uri)
                .credential(azureKeyCredential)
                .directMode(directConnectionConfig, gatewayConnectionConfig);
    }

    @Override
    public CosmosConfig cosmosConfig() {

        log.warn("cosmosConfig, queryMetricsEnabled: " + queryMetricsEnabled);

        return CosmosConfig.builder()
                .responseDiagnosticsProcessor(new ResponseDiagnosticsProcessorImpl())
                .enableQueryMetrics(true)
                .build();
    }

    @Override
    protected String getDatabaseName() {

        log.warn("getDatabaseName returning: " + dbName);
        return dbName;
    }

}