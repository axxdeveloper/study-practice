package com.shooeugenesea.dao;

import com.shooeugenesea.IntegrationTest;
import com.shooeugenesea.entity.Person;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

@SpringBootTest
class PersonDaoTest extends IntegrationTest {

	@Autowired
	private PersonDao dao;

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
