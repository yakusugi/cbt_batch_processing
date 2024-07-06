package com.springbatch.domain;

import java.util.Date;

public class UserSpending {
	
	private Integer spendingId;
	private String email;
	private Date spendingDate;
	private String storeName;
	private String productName;
	private String productType;
	private Double vatRate;
	private Double price;
	private String note;
	private String currencyCode;
	private Integer quantity;
	
	public Integer getSpendingId() {
		return spendingId;
	}
	public void setSpendingId(Integer spendingId) {
		this.spendingId = spendingId;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public Date getSpendingDate() {
		return spendingDate;
	}
	public void setSpendingDate(Date spendingDate) {
		this.spendingDate = spendingDate;
	}
	public String getStoreName() {
		return storeName;
	}
	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	public String getProductType() {
		return productType;
	}
	public void setProductType(String productType) {
		this.productType = productType;
	}
	public Double getVatRate() {
		return vatRate;
	}
	public void setVatRate(Double vatRate) {
		this.vatRate = vatRate;
	}
	public Double getPrice() {
		return price;
	}
	public void setPrice(Double price) {
		this.price = price;
	}
	public String getNote() {
		return note;
	}
	public void setNote(String note) {
		this.note = note;
	}
	public String getCurrencyCode() {
		return currencyCode;
	}
	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}
	public Integer getQuantity() {
		return quantity;
	}
	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	@Override
	public String toString() {
		return "UserSpending [spendingId=" + spendingId + ", email=" + email + ", spendingDate=" + spendingDate
				+ ", storeName=" + storeName + ", productName=" + productName + ", productType=" + productType
				+ ", vatRate=" + vatRate + ", price=" + price + ", note=" + note + ", currencyCode=" + currencyCode
				+ ", quantity=" + quantity + "]";
	}
	
	

	
	
	
	
	
	
	
	
	
	

}
