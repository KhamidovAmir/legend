package com.demp.exceptions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableJpaRepositories(considerNestedRepositories = true)
@ConfigurationPropertiesScan
public class ExceptionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExceptionServiceApplication.class, args);
    }
}
