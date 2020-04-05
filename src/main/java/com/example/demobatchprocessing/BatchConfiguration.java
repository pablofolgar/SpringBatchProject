package com.example.demobatchprocessing;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;


	@Bean
	ItemReader<Person> reader(DataSource dataSource) {
		JdbcPagingItemReader<Person> databaseReader = new JdbcPagingItemReader<>();

		databaseReader.setDataSource(dataSource);
		databaseReader.setPageSize(1);

		PagingQueryProvider queryProvider = createQueryProvider();
		databaseReader.setQueryProvider(queryProvider);

		databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));

		return databaseReader;
	}

	private PagingQueryProvider createQueryProvider() {
		H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();

		queryProvider.setSelectClause("SELECT person_id, first_name,last_name");
		queryProvider.setFromClause("FROM people");
		queryProvider.setSortKeys(sortByApellidoAsc());

		return queryProvider;
	}
	
	private Map<String, Order> sortByApellidoAsc() {
        Map<String, Order> sortConfiguration = new HashMap<>();
        sortConfiguration.put("last_name", Order.ASCENDING);
        return sortConfiguration;
    }

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Person>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)").dataSource(dataSource)
				.build();
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory
				.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(step1)
				.end()
				.build();
	}

	@Bean
	public Step step1(JdbcBatchItemWriter<Person> writer,ItemReader<Person> reader) {
		return stepBuilderFactory
				.get("step1")
				.<Person, Person>chunk(10)
				.reader(reader)
				.processor(processor())
				.writer(writer)
				.build();
	}
}
