package com.shooeugenesea.dao;

import com.shooeugenesea.entity.Version1;
import com.shooeugenesea.entity.Version2;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface Version2Dao extends JpaRepository<Version2, UUID> {
}
