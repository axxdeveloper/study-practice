package com.shooeugenesea.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.UUID;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
public class Version1 {

    @Id
    private UUID id;
    private Integer major;
    private Integer minor;
    private Integer micro;
    private Integer build;

}
