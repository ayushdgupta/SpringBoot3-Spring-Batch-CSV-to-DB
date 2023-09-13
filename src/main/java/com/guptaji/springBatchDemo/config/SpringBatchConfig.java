package com.guptaji.springBatchDemo.config;

import com.guptaji.springBatchDemo.entity.Customer;
import com.guptaji.springBatchDemo.repository.CustomerRepo;
import com.guptaji.springBatchDemo.service.CustomerProcessor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class SpringBatchConfig {

  @Autowired private CustomerRepo customerRepo;

  @Bean
  public FlatFileItemReader<Customer> itemReader() {
    return new FlatFileItemReaderBuilder<Customer>()
        .name("CustomerItemReader")
        .resource(new FileSystemResource("src/main/resources/customers.csv"))
        .linesToSkip(1)
        .delimited()
        .names("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob")
        .fieldSetMapper(
            new BeanWrapperFieldSetMapper<Customer>() {
              {
                setTargetType(Customer.class);
              }
            })
        .build();
  }

  @Bean
  public CustomerProcessor itemProcessor() {
    return new CustomerProcessor();
  }

  @Bean
  public RepositoryItemWriter<Customer> itemWriter() {
    RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
    writer.setRepository(customerRepo);
    writer.setMethodName("save");
    return writer;
  }

  @Bean
  public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("csv-step", jobRepository)
        .<Customer, Customer>chunk(10, transactionManager)
        .reader(itemReader())
        .processor(itemProcessor())
        .writer(itemWriter())
        .taskExecutor(taskExecutor())
        .build();
  }

  @Bean
  public Job importCustomerJob(
      JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new JobBuilder("importCustomerJob", jobRepository)
        .flow(step1(jobRepository, transactionManager))
        .end()
        .build();
  }

  // For Multithreading
  @Bean
  public TaskExecutor taskExecutor() {
    SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
    executor.setConcurrencyLimit(10);
    return executor;
  }
}
