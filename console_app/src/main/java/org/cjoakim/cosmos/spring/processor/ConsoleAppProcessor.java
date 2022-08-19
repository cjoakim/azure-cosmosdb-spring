package org.cjoakim.cosmos.spring.processor;

/**
 * Interface for "processor" classes created and invoked from the DataCommandLineApp
 * main class, a "console app"
 *
 * Chris Joakim, Microsoft, July 2022
 */

public abstract class ConsoleAppProcessor {

    public abstract void process() throws Exception;

}