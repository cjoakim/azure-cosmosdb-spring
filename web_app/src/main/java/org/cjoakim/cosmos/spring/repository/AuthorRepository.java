package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.repository.CosmosRepository;
import org.cjoakim.cosmos.spring.model.Author;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

/**
 *
 * Chris Joakim, Microsoft, July 2022
 */

@Component
@Repository
public interface AuthorRepository extends CosmosRepository<Author, String> {

    Iterable<Author> findByLabel(String label);
}
