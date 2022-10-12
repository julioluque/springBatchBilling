package com.example.demo.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import com.example.demo.model.Student;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public JdbcCursorItemReader<Student> reader() {
		JdbcCursorItemReader<Student> reader = new JdbcCursorItemReader<Student>();
		reader.setDataSource(dataSource);
		reader.setSql("select id, firstName, lastName, email from csvtodbdata");
		reader.setRowMapper(new RowMapper<Student>() {
			@Override
			public Student mapRow(ResultSet rs, int rowNum) throws SQLException {
				Student s = new Student();
				s.setId(rs.getInt("id"));
				s.setFirstName(rs.getString("firstName"));
				s.setLastName(rs.getString("lastName"));
				s.setEmail(rs.getString("email"));
				return s;
			}
		});
		return reader;
	}
	
	@Bean
	public FlatFileItemWriter<Student> writer(){
		FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("C://Users/Julio/wrumboMD/csv_output.xml"));
		DelimitedLineAggregator<Student> aggregator = new DelimitedLineAggregator<>();
		BeanWrapperFieldExtractor<Student> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] {"id","firstName","lastName","email"});
		aggregator.setFieldExtractor(fieldExtractor);
		writer.setLineAggregator(aggregator);
		return writer;
	}

	@Bean
	public Step executeStep() {
		return stepBuilderFactory.get("executeStep")
		.<Student, Student>chunk(10)
		.reader(reader())
		.writer(writer())
		.build();
	}
	
	@Bean
	public Job processJob() {
		return jobBuilderFactory.get("processJob")
		.incrementer(new RunIdIncrementer())
		.flow(executeStep())
		.end()
		.build();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}















