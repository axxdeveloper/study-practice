package com.shooeugenesea.dao;

import com.shooeugenesea.entity.Version4;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface Version4Dao extends JpaRepository<Version4, UUID> {
}
