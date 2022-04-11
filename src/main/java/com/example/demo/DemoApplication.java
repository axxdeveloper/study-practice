package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	//spring.cloud.function.definition=status
	//spring.cloud.stream.bindings.status-in-0.destination=status
	@Bean
	public Consumer<String> status() {
		return status -> System.out.println("Received " + status);
	}

	//spring.cloud.stream.bindings.randomNumberSuffix-in-0.destination=randomNumberSuffix
	//spring.cloud.stream.bindings.randomNumberSuffix-out-0.destination=status
	@Bean
	public Function<String, String> randomNumberSuffix() {
		return val -> val + " => append suffix " + Math.random();
	}

	//spring.cloud.stream.bindings.mydate-out-0.destination=status
	//spring.cloud.stream.poller.fixed-delay=2000
	// custom poller will be in 3.2: https://github.com/spring-cloud/spring-cloud-stream/issues/2280
	@Bean
	public Supplier<Date> mydate() {
		return () -> new Date();
	}


}
