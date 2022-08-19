package org.cjoakim.cosmos.spring;

import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.repository.TripleRepository;
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
 * Chris Joakim, Microsoft, July 2022
 */

@SpringBootApplication
@Slf4j
public class App implements CommandLineRunner, AppConstants {

    @Autowired private ApplicationContext applicationContext;
    @Autowired private TripleRepository tripleRepository;
    @Autowired private CosmosDbLoader cosmosDbLoader;
    @Autowired private RepoQueryProcessor repoQueryProcessor;
    @Autowired private DaoQueryProcessor daoQueryProcessor;

    public static void main(String[] args) {
        AppConfiguration.setCommandLineArgs(args);
        log.warn("main method...");
        SpringApplication.run(App.class, args);
    }

    public void run(String[] args) throws Exception {
        log.warn("run method...");
        AppConfiguration.setCommandLineArgs(args);
        String processType = args[0];
        ConsoleAppProcess processor = null;
        log.warn("run, processType: " + processType);

        try {
            switch (processType) {
                case "transform_raw_data":
                    processor = new RawDataTransformer();
                    processor.process();
                    break;
                case "load_cosmos":
                    cosmosDbLoader.process();
                    break;
                case "springdata_queries":
                    repoQueryProcessor.process();
                    break;
                case "dao_queries":
                    daoQueryProcessor.process();
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
