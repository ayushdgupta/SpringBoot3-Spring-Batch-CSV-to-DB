package com.guptaji.springBatchDemo.repository;

import com.guptaji.springBatchDemo.entity.Customer;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepo extends JpaRepository<Customer, Integer> {}
