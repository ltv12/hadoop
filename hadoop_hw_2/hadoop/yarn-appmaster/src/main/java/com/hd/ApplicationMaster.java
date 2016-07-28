package com.hd;

import com.hd.controller.ConfigurationController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.hd")
public class ApplicationMaster {

    public static void main(String[] args) {

        SpringApplication.run(new Class<?>[] { ApplicationMaster.class,
            ConfigurationController.class }, args);
    }
}
