package org.cjoakim.cosmos.spring.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * This is the entry-point to this Spring Web Application, as denoted by the
 * @SpringBootApplication annotation.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@SpringBootApplication
@ComponentScan(basePackages = { "org.cjoakim" })
public class Application {

    public static void main(String[] args) {

         SpringApplication.run(Application.class, args);
    }
}
