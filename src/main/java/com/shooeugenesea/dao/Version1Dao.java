package com.shooeugenesea.dao;

import com.shooeugenesea.entity.Version1;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface Version1Dao extends JpaRepository<Version1, UUID> {
}
