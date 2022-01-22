package com.shooeugenesea.dao;

import com.shooeugenesea.entity.Version2;
import com.shooeugenesea.entity.Version3;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface Version3Dao extends JpaRepository<Version3, UUID> {
}
