package com.shooeugenesea.dao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.shooeugenesea.entity.Person;

import java.util.UUID;

@Repository
public interface PersonDao extends JpaRepository<Person, UUID> {
}
