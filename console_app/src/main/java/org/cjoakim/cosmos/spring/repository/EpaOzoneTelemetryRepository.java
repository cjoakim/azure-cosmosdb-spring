package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.repository.CosmosRepository;
import com.azure.spring.data.cosmos.repository.Query;
import org.cjoakim.cosmos.spring.model.EpaOzoneTelemetryEvent;
import org.cjoakim.cosmos.spring.model.Library;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Repository for class EpaOzoneTelemetryEvent.
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Repository
public interface EpaOzoneTelemetryRepository extends CosmosRepository<EpaOzoneTelemetryEvent, String> {

//    Iterable<Library> findByPkAndTenant(String pk, String tenant);
//
//    Iterable<Library> findByPkAndTenantAndDoctype(String pk, String tenant, String doctype);


    @Query("select value count(1) from c")
    long countAllDocuments();

}
