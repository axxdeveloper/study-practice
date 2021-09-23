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
public class Person {

    @Id
    private UUID id;
    private String name;

    public Person(String name) {
        this.name = name;
        this.id = UUID.randomUUID();
    }

}
