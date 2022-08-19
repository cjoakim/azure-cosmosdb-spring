package org.cjoakim.cosmos.spring.processor;

import com.azure.spring.data.cosmos.core.CosmosTemplate;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.io.FileUtil;
import org.cjoakim.cosmos.spring.model.Library;
import org.cjoakim.cosmos.spring.model.Triple;
import org.cjoakim.cosmos.spring.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Instances of this class are used for ad-hoc testing and development
 * of CosmosDB Spring Data Repository functionality within the (batch)
 * DataCommandLineApp,
 *
 * Chris Joakim, Microsoft, July 2022
 */

@Component
@Slf4j
public class RepoQueryProcessor extends ConsoleAppProcessor implements AppConstants {

    private AuthorRepository authorRepository = null;

    private LibraryRepository libraryRepository = null;

    private MaintainerRepository maintainerRepository = null;

    private TripleRepository tripleRepository = null;

    private CosmosTemplate template;

    @Autowired
    public RepoQueryProcessor(
            AuthorRepository ar,
            LibraryRepository lr,
            MaintainerRepository mr,
            TripleRepository tr,
            CosmosTemplate t) {
        super();
        this.authorRepository = ar;
        this.libraryRepository = lr;
        this.maintainerRepository = mr;
        this.tripleRepository = tr;
        this.template = t;
        log.warn("RepoQueryProcessor autowired constructor called");
    }

    public void process() throws Exception {

        String tenant = AppConfiguration.getTenant();
        String lob    = AppConfiguration.getLineOfBusiness();
        log.warn("process, tenant: " + tenant);

        ArrayList<String> libsOfInterest = new ArrayList<String>();
        libsOfInterest.add("tedious");
        libsOfInterest.add("adal-node");
        libsOfInterest.add("express");
        libsOfInterest.add("m26-js");

        FileUtil fu = new FileUtil();

        // Find the Libraries
        if (true) {
            log.warn("---");
            log.warn("process libraryRepository.findByPkAndTenant");
            for (int i = 0; i < libsOfInterest.size(); i++) {
                String libName = libsOfInterest.get(i);
                Iterable<Library> iterable = libraryRepository.findByPkAndTenant(libName, tenant);
                iterable.forEach(doc -> {
                    try {
                        String name = doc.getName();
                        String outfile = "data/refined/" + name + ".json";
                        fu.writeJson(doc, outfile, true, true);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
            }
        }

        // Find the corresponding Triples
        if (true) {
            log.warn("---");
            log.warn("process tripleRepository.findByTenantAndSubjectLabel");
            for (int i = 0; i < libsOfInterest.size(); i++) {
                String libName = libsOfInterest.get(i);
                Iterable<Triple> iterable = tripleRepository.findByTenantAndSubjectLabel(tenant, libName);
                iterable.forEach(doc -> {
                    try {
                        System.out.println("doc: " + doc.asJson(true));
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }

        if (true) {
            log.warn("---");
            log.warn("process tripleRepository.getByTenantAndSubjectLabels");
            // Find with more complex SQL in Repository method
            ArrayList<String> labels = new  ArrayList<String>();
            labels.add("@azure/cosmos");
            labels.add("@azure/cosmos-sign");
            labels.add("@azure/amqp-common");

            for (int i = 0; i < libsOfInterest.size(); i++) {
                String libName = libsOfInterest.get(i);
                Iterable<Triple> iterable = tripleRepository.getByTenantAndSubjectLabels(tenant, labels);
                iterable.forEach(doc -> {
                    try {
                        System.out.println("doc: " + doc.asJson(true));
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }

        if (true) {
            log.warn("---");
            log.warn("process tripleRepository.getNumberOfDocsWithSubjectLabel");
            long count = tripleRepository.getNumberOfDocsWithSubjectLabel("m26-js");
            log.warn("count of getNumberOfDocsWithSubjectLabel: " + count);
            log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }

        if (true) {
            log.warn("---");
            log.warn("process libraryRepository.findByPkAndTenantAndDoctype");
            Iterable<Library> iterable = libraryRepository.findByPkAndTenantAndDoctype(
                    "tedious", tenant, "library");
            Iterator<Library> it = iterable.iterator();
            long docCount = 0;
            while (it.hasNext()) {
                Library lib = it.next();
                System.out.println(lib.asJson(true));
                docCount++;
            }
            log.warn("findByTenantAndDoctypeAndPk count: " + docCount);
            log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }

        if (true) {
            log.warn("---");
            log.warn("process template count");
            // example of using CosmosTemplate outside of a Repository
            log.warn("template: " + template);
            long count = template.count("altgraph");
            log.warn("doc count in altgraph container: " + count);
            log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }

        log.warn("countAllDocuments:   " + tripleRepository.countAllDocuments());
        log.warn("countAllTriples:     " + tripleRepository.countAllTriples());
        log.warn("countAllLibraries:   " + tripleRepository.countAllLibraries());
        log.warn("countAllAuthors:     " + tripleRepository.countAllAuthors());
        log.warn("countAllMaintainers: " + tripleRepository.countAllMaintainers());
    }
}
