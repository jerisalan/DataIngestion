
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.Provider;
import java.security.Security;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PushToKafka {

	private static final String UNZIPPEDFOLDERNAME = "unzipped";
	private static Logger logger = Logger.getLogger(PushToKafka.class);
	private static BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(2);
	
	public static void main(String[] args) throws Exception
	{
		Properties properties = new Properties();
		properties.put("log4j.rootLogger", "INFO, Appender1");
		properties.put("log4j.appender.Appender1", "org.apache.log4j.ConsoleAppender");
		properties.put("log4j.appender.Appender1.layout", "org.apache.log4j.PatternLayout");
		properties.put("log4j.appender.Appender1.layout.ConversionPattern", "%-7p %d [%t] %c %x - %m%n");
		PropertyConfigurator.configure(properties);
		System.setProperty("https.protocols", "TLSv1.1");
		System.setProperty("java.protocol.handler.pkgs", "com.sun.net.ssl.internal.www.protocol");
		try
	    {
	    //if we have the JSSE provider available, 
	    //and it has not already been
	    //set, add it as a new provide to the Security class.
	    Class clsFactory = Class.forName("com.sun.net.ssl.internal.ssl.Provider");
	    if( (null != clsFactory) && (null == Security.getProvider("SunJSSE")) )
	        Security.addProvider((Provider)clsFactory.newInstance());
	    }
	    catch( ClassNotFoundException cfe )
	    {
	      throw new Exception("Unable to load the JSSE SSL stream handler." +  
	        "Check classpath."  + cfe.toString());
	    }
   
		Properties props = new Properties();
		props.put("bootstrap.servers", "152.46.19.55:9092,152.46.20.245:9092,152.1.13.146:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
		File edgarURLs = new File("edgarLOGS.txt");
		if(!edgarURLs.exists())
		{
			logger.error("Error occurred: edgarLOGS.txt file doesn't exist");
		}
		
		queue.add(1);
		queue.add(2);
		
		try(Producer<String, String> producer = new KafkaProducer<>(props);
				BufferedReader bf = new BufferedReader(new FileReader(edgarURLs)))
		{
			int fileNumber = 1;
			String line;
			
			while((line = bf.readLine()) != null)
			{
				final String fileName = fileNumber + ".zip";
				final String url = line;

				File destination = new File(fileName);
				File csvFile = null;
				try 
				{
//					URL source = new URL("https://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr4/log20161231.zip");
					URL source = new URL("https://" + url);
					logger.info("Now downloading file from: " + source);
					FileUtils.copyURLToFile(source, destination);
					String csvFilePath = unzip(destination);
					if(csvFilePath==null)
						throw new Exception("Unable to read csv file from the unzipped file");
					csvFile = new File(csvFilePath);
					logger.info("Now pushing downloaded file to kafka: " + csvFilePath);
					parseFileAndPushToKafka(producer, csvFilePath);
				}
				catch (Exception e)
				{
					logger.error("Error occurred", e);
				}
				finally
				{
					destination.delete();
					if(csvFile != null)
						csvFile.delete();
					
					logger.info("Quitting thread for: " + destination);
				}
				
				fileNumber++;
			}
		}
	}
	
	private static void parseFileAndPushToKafka(Producer<String, String> producer, String csvFile) throws IOException, InterruptedException, ExecutionException, TimeoutException 
	{
		List<Future<RecordMetadata>> futureList = new LinkedList<>();
		ObjectMapper objectMapper = new ObjectMapper();
		InputStream inputFS = new FileInputStream(csvFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
		String[] csvHeaders = br.readLine().split(",");
		// skip the header of the csv
		br.lines().forEach((line) -> {
			
			
			int topicNumber = 1;
			try {
				topicNumber = queue.take();
				
			} catch (InterruptedException e1) {	}

			String topic = "json-topic" + topicNumber;
			
			ObjectNode jsonObject = objectMapper.createObjectNode();
			String[] csvValues = line.split(",");
			for(int i=0;i<csvHeaders.length - 1;i++)
			{
				String value = csvValues[i];
				
				//Currently passing everything as String
				jsonObject.put(csvHeaders[i], value);
			}
			
			Integer randomInt = null;
			try{
				randomInt = new Random().ints().findFirst().getAsInt();
			} catch(Exception e)
			{}
			String partitionString = randomInt == null ? "partition" : Integer.toString(randomInt);
			
			// Pushing row to Kafka
			futureList.add(producer.send(new ProducerRecord<String, String>(topic, partitionString, jsonObject.toString())));
			queue.add(topicNumber);
		});
		br.close();

		// Error checking -- Basic, just throw error
		for(Future<RecordMetadata> future : futureList)
			future.get(5, TimeUnit.SECONDS);
	} 

	public static String unzip(File destination) throws IOException
	{
		byte[] buffer = new byte[1024];
		createFolder(UNZIPPEDFOLDERNAME);
        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(destination)))
        {
	        ZipEntry zipEntry = zis.getNextEntry();
	        while(zipEntry != null)
	        {
	        	String fileName = zipEntry.getName();
	        	
	        	if(!fileName.endsWith(".csv"))
	        	{
	        		zipEntry = zis.getNextEntry();
	        		continue;
	        	}
	        	
	            File newFile = new File("unzipped/" + fileName);
	            FileOutputStream fos = new FileOutputStream(newFile);
	            int len;
	            while ((len = zis.read(buffer)) > 0) {
	                fos.write(buffer, 0, len);
	            }
	            fos.close();
	            
	        	zipEntry = zis.getNextEntry();
	        	
	        	return newFile.getAbsolutePath();
	        }
        }
        return null;
	}
	
	public static void createFolder(String folderName)
	{
		File theDir = new File(folderName);
		if (!theDir.exists()) {
	        theDir.mkdir();
		}
	}

}
