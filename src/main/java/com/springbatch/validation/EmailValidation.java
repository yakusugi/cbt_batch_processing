package com.springbatch.validation;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.springbatch.domain.UsersRowMapper;
import com.springbatch.domain.Users;

public class EmailValidation {
	
	@Autowired
    public DataSource dataSource;
	
	public JdbcCursorItemReader<Users> jdbcCursorItemReader(@Value("#{jobParameters['email']}") String email) {
		JdbcCursorItemReader<Users> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
        itemReader.setSql("select * from users where email = email = '" + email + "'");
        itemReader.setRowMapper(new UsersRowMapper());
        return (JdbcCursorItemReader<Users>) itemReader;
    }
	
	public boolean isEmailValid(String email) {
	    JdbcCursorItemReader<Users> itemReader = jdbcCursorItemReader(email);

	    try {
	        itemReader.afterPropertiesSet(); // Initialize the reader
	        Users user = new Users();

	        // Read the first user with the provided email (if any)
	        while ((user = itemReader.read()) != null) {
	            if (user.getEmail().equalsIgnoreCase(email)) {
	                return true; // Email exists in the database
	            }
	        }
	    } catch (Exception e) {
	        // Handle any exceptions, such as database connectivity issues
	        e.printStackTrace();
	        return false; // An error occurred during validation
	    } finally {
	        try {
	            itemReader.close();
	        } catch (Exception e) {
	            // Handle closing the reader
	            e.printStackTrace();
	        }
	    }

	    return false; // Email not found in the database
	}

}
