package com.shooeugenesea;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@DirtiesContext // used to re-initialize applicationContext, because testcontainer will be restarted with different exposed ports
@Testcontainers
@ContextConfiguration(initializers = IntegrationTest.TestcontainersInitializer.class)
public abstract class IntegrationTest {

    @Container
    protected static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
            .withDatabaseName("test");
            
	public static class TestcontainersInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext,
                    "spring.datasource.url=" + postgreSQLContainer.getJdbcUrl(),
                    "spring.datasource.username=" + postgreSQLContainer.getUsername(),
                    "spring.datasource.password=" + postgreSQLContainer.getPassword()
            );
        }
    }

	@BeforeAll
	public static void beforeClass() {
		postgreSQLContainer.start();
	}

	@AfterAll
	public static void afterClass() {
		postgreSQLContainer.stop();
	}

	@Test
	public void test() {
		Assert.assertTrue(postgreSQLContainer.isRunning());
	}

}
