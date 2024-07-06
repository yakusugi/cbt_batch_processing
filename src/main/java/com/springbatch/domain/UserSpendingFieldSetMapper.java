package com.springbatch.domain;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class UserSpendingFieldSetMapper implements FieldSetMapper<UserSpending> {

	@Override
	public UserSpending mapFieldSet(FieldSet fieldSet) throws BindException {
		// TODO Auto-generated method stub
		UserSpending spending = new UserSpending();
		spending.setSpendingId(fieldSet.readInt("spending_id"));
		spending.setEmail(fieldSet.readString("email"));
		spending.setSpendingDate(fieldSet.readDate("spending_date"));
		spending.setStoreName(fieldSet.readString("store_name"));
		spending.setProductName(fieldSet.readString("product_name"));
		spending.setProductType(fieldSet.readString("product_type"));
		spending.setVatRate(fieldSet.readDouble("vat_rate"));
		spending.setPrice(fieldSet.readDouble("price"));
		spending.setNote(fieldSet.readString("note"));
		spending.setCurrencyCode(fieldSet.readString("currency_code"));
		spending.setQuantity(fieldSet.readInt("quantity"));
		
		return spending;
	}

}
