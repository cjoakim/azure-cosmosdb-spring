package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.repository.CosmosRepository;
import org.cjoakim.cosmos.spring.model.Maintainer;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

/**
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Repository
public interface MaintainerRepository extends CosmosRepository<Maintainer, String> {

}
