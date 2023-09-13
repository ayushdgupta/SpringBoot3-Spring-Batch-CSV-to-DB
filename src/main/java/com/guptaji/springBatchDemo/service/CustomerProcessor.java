package com.guptaji.springBatchDemo.service;

import com.guptaji.springBatchDemo.entity.Customer;

import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

  @Override
  public Customer process(Customer customer) throws Exception {
    customer.setCountry("India");
    return customer;
  }
}
