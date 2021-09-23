package com.shooeugenesea;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.shooeugenesea.dao.PersonDao;
import com.shooeugenesea.entity.Person;

import java.util.UUID;

@Testcontainers
@SpringBootTest
@ContextConfiguration(initializers = PersonDaoTest.DockerPostgreDataSourceInitializer.class)
class PersonDaoTest {

	@Autowired
	private PersonDao dao;

	@Container
    static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
            .withDatabaseName("test");

	public static class DockerPostgreDataSourceInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext,
                    "spring.datasource.url=" + postgreSQLContainer.getJdbcUrl(),
                    "spring.datasource.username=" + postgreSQLContainer.getUsername(),
                    "spring.datasource.password=" + postgreSQLContainer.getPassword()
            );
			System.out.println(applicationContext.getEnvironment());
        }
    }
	
	@BeforeClass
	public static void init() {
		postgreSQLContainer.start();
	}

	@AfterClass
	public static void after() {
		postgreSQLContainer.stop();
	}

	@Test
	void testCrud() {
		Person save = dao.save(new Person(UUID.randomUUID().toString()));
		Person found = dao.findById(save.getId()).get();
		Assert.assertEquals(found.getName(), save.getName());
		
		Person update = new Person(found.getId(), UUID.randomUUID().toString());
		Person found2 = dao.save(update);
		Assert.assertEquals(found2.getName(), update.getName());
		
		dao.delete(found2);
		Assert.assertTrue(dao.findById(found2.getId()).isEmpty());
	}

}
