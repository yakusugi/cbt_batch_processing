package com.springbatch.domain;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class UsersRowMapper implements RowMapper<Users> {

	@Override
	public Users mapRow(ResultSet rs, int rowNum) throws SQLException {
		System.out.println("ResultSet object = " + rs);
		Users users = new Users();
		users.setUserName(rs.getString("user_name"));
		users.setEmail(rs.getString("email"));
		users.setPassword(rs.getString("password"));
		
		return users;
	}

}
