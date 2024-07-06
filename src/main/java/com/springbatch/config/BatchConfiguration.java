package com.springbatch.config;



import com.springbatch.domain.UserSpending;
import com.springbatch.domain.UserSpendingRowMapper;
import com.springbatch.tasklet.CurrencyExchangeApiTasklet;
import com.springbatch.tasklet.ExitCodeCheckingTasklet;
import com.springbatch.tasklet.LatestFileTasklet;
import com.springbatch.utility.FileNameSettingListener;
import com.springbatch.validation.EmailValidation;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.propertyeditors.CustomDateEditor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.batch.item.ItemStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Collectors;

import javax.batch.api.chunk.ItemProcessor;
import javax.batch.runtime.StepExecution;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	private LatestFileTasklet latestFileTasklet;

    public BatchConfiguration(LatestFileTasklet latestFileTasklet) {
        this.latestFileTasklet = latestFileTasklet;
    }

    // Validation check (currency exist)
	@Bean
	@JobScope
	public ItemStreamReader<UserSpending> currencyExistingValidation(
			@Value("#{jobParameters['email']}") String email,
			@Value("#{jobParameters['sourceCurrencyCode']}") String sourceCurrencyCode,
			@Value("#{jobParameters['targetCurrencyCode']}") String targetCurrencyCode) {

		JdbcCursorItemReader<UserSpending> itemReader = new JdbcCursorItemReader<>();
		itemReader.setDataSource(dataSource);
		itemReader.setSql(getSqlFromFileForValidation("sql/user_spending_query.sql", email, sourceCurrencyCode,
				targetCurrencyCode));
		itemReader.setRowMapper(new UserSpendingRowMapper());
		return itemReader;
	}

	private String getSqlFromFileForValidation(String filePath, String email, String sourceCurrencyCode,
			String targetCurrencyCode) {
		// Load SQL from file
		Resource resource = new ClassPathResource(filePath);
		String sqlTemplate = null;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
			sqlTemplate = reader.lines().collect(Collectors.joining(System.lineSeparator()));
		} catch (IOException e) {
			// Handle exception
			e.printStackTrace();
		}

		// Replace placeholders with actual values
		return String.format(sqlTemplate, email, sourceCurrencyCode, targetCurrencyCode);
	}

	// currency calc and sum
	@Bean
	@JobScope
	public ItemStreamReader<UserSpending> targetCurrencyCsv(
			@Value("#{jobParameters['email']}") String email,
			@Value("#{jobParameters['dateFrom']}") Date dateFrom,
			@Value("#{jobParameters['dateTo']}") Date dateTo,
			@Value("#{jobParameters['targetCurrencyCode']}") String targetCurrencyCode

	) {

		JdbcCursorItemReader<UserSpending> itemReader = new JdbcCursorItemReader<>();
		itemReader.setDataSource(dataSource);
		itemReader.setSql(getSqlFromFileTargetCurrency("sql/calc_sum.sql", dateFrom, dateTo, targetCurrencyCode, email));
		itemReader.setRowMapper(new UserSpendingRowMapper());
		return itemReader;
	}

	private String getSqlFromFileTargetCurrency(String filePath, Date dateFrom, Date dateTo, String targetCurrencyCode,
			String email) {
		// Format dates to Strings
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); // Adjust format as needed
		String formattedDateFrom = dateFormat.format(dateFrom);
		String formattedDateTo = dateFormat.format(dateTo);

		// Load SQL from file
		Resource resource = new ClassPathResource(filePath);
		String sqlTemplate;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
			sqlTemplate = reader.lines().collect(Collectors.joining(System.lineSeparator()));
		} catch (IOException e) {
			// Handle exception
			e.printStackTrace();
			return null;
		}

		// Replace placeholders with actual values
		return String.format(sqlTemplate, formattedDateFrom, formattedDateTo, targetCurrencyCode, email);
	}

	@Bean
	@JobScope
	public ItemStreamWriter<UserSpending> flatFileItemWriter(FileNameSettingListener fileNameSettingListener) throws Exception {
		FlatFileItemWriter<UserSpending> itemWriter = new FlatFileItemWriter<>();
//		itemWriter.setResource(new FileSystemResource("src/main/resources/data/Product_Details_Output9.csv"));
		
		// Format the current date and time
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
//        String formattedDate = dateFormat.format(new Date());
//        String fileName = "src/main/resources/data/Product_Details_" + formattedDate + ".csv";
		
		String fileName = fileNameSettingListener.getFileName();
	    itemWriter.setResource(new FileSystemResource(fileName));
        
     // Set the resource to a new file with the formatted date in its name
//        itemWriter.setResource(new FileSystemResource("src/main/resources/data/Product_Details_" + formattedDate + ".csv"));

		DelimitedLineAggregator<UserSpending> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		BeanWrapperFieldExtractor<UserSpending> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] { "spendingId", "email", "spendingDate", "storeName", "productName",
				"productType", "vatRate", "price", "note", "currencyCode", "quantity" });

		lineAggregator.setFieldExtractor(fieldExtractor);

		itemWriter.setLineAggregator(lineAggregator);
		
//		stepExecution.getExecutionContext().putString("csvFileName", fileName);

		return itemWriter;
	}
	
	@Bean
	@JobScope
	public FlatFileItemReader<UserSpending> csvFileItemReader() throws Exception {
        FlatFileItemReader<UserSpending> reader = new FlatFileItemReader<>();

        latestFileTasklet.execute(null, null);
        Path latestFilePath = latestFileTasklet.getLatestFilePath();
        String latestFileName = null;
        if (latestFilePath != null) {
            System.out.println("Latest file to process: " + latestFilePath.getFileName());
            // Add your processing logic here
            latestFileName = latestFileTasklet.getLatestFileName();
        } else {
            System.out.println("No latest file found to process.");
        }

		System.out.println("test333 src/main/resources/data/" + latestFileName);
        reader.setResource(new FileSystemResource("src/main/resources/data/" + latestFileName));

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames(new String[]{"spendingId", "email", "spendingDate", "storeName", "productName",
                "productType", "vatRate", "price", "note", "currencyCode", "quantity"});

        BeanWrapperFieldSetMapper<UserSpending> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(UserSpending.class);

        // Add a custom date editor to handle date parsing
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        fieldSetMapper.setCustomEditors(Collections.singletonMap(Date.class, new CustomDateEditor(dateFormat, false)));

        DefaultLineMapper<UserSpending> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);

        return reader;
    }

	// for reading
	@Bean
	@JobScope
	public Step step1(@Qualifier("currencyExistingValidation") ItemReader<UserSpending> reader,
			ItemWriter<UserSpending> writer, FileNameSettingListener fileNameSettingListener) {
		return stepBuilderFactory.get("step1").<UserSpending, UserSpending>chunk(3)
				.reader(reader)
				.writer(writer)
				.listener(fileNameSettingListener)
				.build();
	}

	@Bean
	@JobScope
	public Step step2(@Qualifier("targetCurrencyCsv") ItemReader<UserSpending> reader, ItemWriter<UserSpending> writer) {
		return stepBuilderFactory.get("step2").<UserSpending, UserSpending>chunk(3)
				.reader(reader)
				.writer(writer)
				.build();
	}
	
	@Bean
	public Step step3() throws Exception {
	    return stepBuilderFactory.get("step3")
	            .<UserSpending, UserSpending>chunk(10)
	            .reader(csvFileItemReader())
	            .writer(list -> {
	                for (UserSpending userSpending : list) {
	                    System.out.println(userSpending); // You can process the data as needed
	                }
	            })
	            .build();
	}

	@Bean
	public Step latestFileStep() {
		return stepBuilderFactory.get("latestFileStep")
				.tasklet(latestFileTasklet)
				.build();
	}


	@Bean
	public Step currencyExchangeStep() {
		return stepBuilderFactory.get("currencyExchangeStep")
				.tasklet(new CurrencyExchangeApiTasklet())
				.build();
	}

	@Bean
	public Job firstJob(Step step1, Step step2, Step step3, Step latestFileStep, Step currencyExchangeStep) {
		return this.jobBuilderFactory.get("job1")
				.start(step1)
				.next(step2)
				.next(step3)
				.next(latestFileStep)
				.next(currencyExchangeStep)
				.build();
	}
}