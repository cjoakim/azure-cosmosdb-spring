package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.repository.CosmosRepository;
import org.cjoakim.cosmos.spring.model.Library;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

/**
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Repository
public interface LibraryRepository extends CosmosRepository<Library, String> {

    Iterable<Library> findByPkAndTenant(String pk, String tenant);

    Iterable<Library> findByPkAndTenantAndDoctype(String pk, String tenant, String doctype);

}
