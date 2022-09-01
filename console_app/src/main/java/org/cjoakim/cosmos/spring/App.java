package org.cjoakim.cosmos.spring;

import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.processor.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * This is the entry=point for this Spring Boot application.
 * It is a "console app" due to the CommandLineRunner interface.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@SpringBootApplication
@Slf4j
public class App implements CommandLineRunner, AppConstants {

    @Autowired private ApplicationContext applicationContext;
    //@Autowired private TripleRepository tripleRepository;

    @Autowired private SpringDataLoaderProcessor springDataLoader;
    @Autowired private SpringDataQueryProcessor springDataQueryProcessor;
    @Autowired private SpringDataDeleteAllProcessor springDataDeleteProcessor;
    @Autowired private RepoQueryProcessor repoQueryProcessor;
    @Autowired private SdkDaoQueryProcessor sdkDaoQueryProcessor;

    public static void main(String[] args) {
        AppConfiguration.setCommandLineArgs(args);
        log.warn("main method...");
        SpringApplication.run(App.class, args);
    }

    public void run(String[] args) throws Exception {
        log.warn("run method...");
        AppConfiguration.setCommandLineArgs(args);
        String processType = args[0];
        log.warn("run, processType: " + processType);

        try {
            switch (processType) {

                case "transform_raw_epa_ozone_data":
                    EpaRawOzoneDataProcessor epaOzoneDataProcessor = new EpaRawOzoneDataProcessor();
                    epaOzoneDataProcessor.setSkipCount(Long.parseLong(args[1]));
                    epaOzoneDataProcessor.setMaxRecords(Long.parseLong(args[2]));
                    epaOzoneDataProcessor.setPartitionKeyStrategy(args[3]);
                    epaOzoneDataProcessor.process();
                    break;

                case "load_telemetry_with_spring_data":
                    springDataLoader.setSkipCount(Long.parseLong(args[1]));
                    springDataLoader.setMaxRecords(Long.parseLong(args[2]));
                    springDataLoader.setLoadType(args[3]);
                    springDataLoader.setInfile(args[4]);
                    springDataLoader.process();
                    break;

                case "query_telemetry_with_spring_data":
                    springDataQueryProcessor.setQueryTypes(args[1]);
                    springDataQueryProcessor.process();
                    break;

                case "delete_all_documents_with_spring_data":
                    springDataDeleteProcessor.setContainer(args[1]);
                    springDataDeleteProcessor.process();
                    break;

                case "load_telemetry_data_with_sdk_bulk_load":
                    // TODO - implement
                    //springDataLoader.setSkipCount(Long.parseLong(args[1]));
                    //springDataLoader.setMaxRecords(Long.parseLong(args[2]));
                    //springDataLoader.setLoadType(args[3]);
                    //springDataLoader.setInfile(args[4]);
                    //springDataLoader.process();
                    break;

                case "query_telemetry_with_sdk":
                    sdkDaoQueryProcessor.setContainer(args[1]);
                    sdkDaoQueryProcessor.process();
                    break;

                default:
                    log.error("unknown CLI process name: " + processType);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        finally {
            log.warn("spring app exiting");
            SpringApplication.exit(this.applicationContext);
            log.warn("spring app exit completed");
        }
    }
}
