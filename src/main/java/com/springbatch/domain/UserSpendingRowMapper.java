package com.springbatch.domain;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class UserSpendingRowMapper implements RowMapper<UserSpending> {

	@Override
	public UserSpending mapRow(ResultSet rs, int rowNum) throws SQLException {
		System.out.println("ResultSet object = " + rs);
		UserSpending spending = new UserSpending();
		spending.setSpendingId(rs.getInt("spending_id"));
		spending.setEmail(rs.getString("email"));
		spending.setSpendingDate(rs.getDate("spending_date"));
		spending.setStoreName(rs.getString("store_name"));
		spending.setProductName(rs.getString("product_name"));
		spending.setProductType(rs.getString("product_type"));
		spending.setVatRate(rs.getDouble("vat_rate"));
		spending.setPrice(rs.getDouble("price"));
		spending.setNote(rs.getString("note"));
		spending.setCurrencyCode(rs.getString("currency_code"));
		spending.setQuantity(rs.getInt("quantity"));
		
		return spending;
	}

}
