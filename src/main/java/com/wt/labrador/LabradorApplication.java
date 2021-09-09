package com.wt.labrador;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author 一贫
 * @date 2021/8/19
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@EnableConfigurationProperties
public class LabradorApplication {
    public static void main(String[] args) {
        SpringApplication.run(LabradorApplication.class, args);
    }
}
