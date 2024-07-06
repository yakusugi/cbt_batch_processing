package com.springbatch.domain;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

public class ProductItemPreparedStatementSetter implements ItemPreparedStatementSetter<UserSpending> {

	@Override
	public void setValues(UserSpending item, PreparedStatement ps) throws SQLException {
		// TODO Auto-generated method stub
		ps.setInt(1, item.getSpendingId());
		ps.setString(2, item.getEmail());
		ps.setDate(3, (Date) item.getSpendingDate());
		ps.setString(4, item.getStoreName());
		ps.setString(1, item.getProductName());
		ps.setString(1, item.getProductType());
		ps.setDouble(1, item.getVatRate());
		ps.setDouble(1, item.getPrice());
		ps.setString(1, item.getNote());
		ps.setString(1, item.getCurrencyCode());
		ps.setInt(1, item.getQuantity());
	}

}
