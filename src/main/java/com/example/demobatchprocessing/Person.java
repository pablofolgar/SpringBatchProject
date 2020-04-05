package com.example.demobatchprocessing;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Person {
	
	private String lastName;
	private String firstName;

	public Person() {
	}
}
