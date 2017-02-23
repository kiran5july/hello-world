package com.kiran.avro;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

//import com.kiran.avro.User;

public class AvroRecordUsingSchema_AvroArray {
	
	public Schema.Parser parser = new Schema.Parser();
	public Schema schema;
	//public Schema schemaName;
	
	private void generateSchemaFromFile(String sSchemaFileName) {
		try{
			//parser = new Schema.Parser();
			schema = parser.parse(getClass().getResourceAsStream(sSchemaFileName));
		}catch(Exception e){}
	}
	
	private void generateSchemaFromText() {
		try{
			//parser = new Schema.Parser();
/*			schema = new Schema.Parser().parse(
			//schemaName = new Schema.Parser().parse(
					"{\"name\": \"id\", \"type\": \"int\"}," +
					"{\"name\":\"fullname\", \"type\":\"record\", " +
							"\"fields\": [{\"name\":\"first\", \"type\":\"string\"}, " +
										 "{\"name\":\"last\", \"type\":\"string\"} " +
										 "]}"
					);*/
			
			schema = new Schema.Parser().parse(
					"{" +
					//"\"namespace\": \"com.kiran.avro\"," +
					"\"type\": \"record\"," +
					"\"name\": \"User\"," +
					"\"fields\": [" +
						"{\"name\": \"id\", \"type\": \"int\"}," +
						//"{\"name\": \"name\", \"type\": \"array\", \"items\":\"string\"}" + //array declaration
						"{\"name\":\"name\", \"type\": { \"type\":\"record\", \"name\":\"fullname\"," +
														"\"fields\": [{\"name\":\"first\", \"type\":\"string\"}, " +
																 "{\"name\":\"last\", \"type\":\"string\"} " +
													 	"]}" +
						"}" +
					" ]" +
					"}"
				 ); 
		}catch(Exception e){
			System.out.println("Exception during schema generation: "+ e.toString());
		}
	}
	
	public void readAvroDataFromFile(String sAvroDataFileName){
		
		
		System.out.println("Reading schema...");
		try{

			GenericRecord oGenRec = new GenericData.Record(schema);

			// Deserialize Users from disk
			File sAvroDataFile = new File(sAvroDataFileName);
			System.out.println("Reading Avro records from disc/file......");
			DatumReader<GenericRecord> userDatumReader = new SpecificDatumReader<GenericRecord>(schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(sAvroDataFile, userDatumReader);
			try{

			int iCounter = 0;
			while (dataFileReader.hasNext()) {
				oGenRec = dataFileReader.next(oGenRec);
				System.out.println("User#:"+(iCounter++)+"->" + oGenRec.get("id")+"/"+oGenRec.get("name.first")+"/"+oGenRec.get("name.last"));
				
			}
			}catch(IOException e){
				StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		        StackTraceElement oStk = stacktrace[1];
				System.out.println("***Exception in method:"+oStk.getMethodName()+"\n****"+e.toString());
			}finally{
				dataFileReader.close();
			}
			System.out.println("Done..");
		}catch(Exception e){
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement oStk = stacktrace[1]; //method name
			System.out.println("***Exception in method:"+oStk.getMethodName()+"\n****"+e.toString());
		}	
				
	}
		
	public void processAvroSchemaInMemory(String sSchemaFileName){

		
		System.out.println("Put values into record.");
		
		GenericRecord oGenRec = new GenericData.Record(schema);
		oGenRec.put("id", 1000);
		oGenRec.put("name.first", "Tom");
		oGenRec.put("name.last", "Jerry");
		oGenRec.put("profession", "SW");
		oGenRec.put("email", "tom@tom.com");
		
		try{
		//Write data of schema into in-memory
		// vv AvroGenericRecordSerialization
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
	    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
	    writer.write(oGenRec, encoder);
	    encoder.flush();
	    out.close();
	    // ^^ AvroGenericRecordSerialization

	    // vv AvroGenericRecordDeserialization
	    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
	    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
	    GenericRecord result = reader.read(null, decoder);
	    System.out.println("Record:"+result.get("id")+"/"+result.get("name"));
	    //assertThat(result.get("id").toString(), is("R"));
	    // ^^ AvroGenericRecordDeserialization
	    System.out.println("Done..");
		}catch(IOException e){
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement oStk = stacktrace[1]; //method name
			System.out.println("***Exception in method:"+oStk.getMethodName()+"\n****"+e.toString());
		}
	    
	}
	
	public void writeAvroDataToFile(String sAvroDataFileName){
	    //Deserialize to a file
		File file = new File(sAvroDataFileName);
		//GenericRecord oRecName = new GenericData.Record(schemaName);
		//oRecName.put("first", "Tom");
		//oRecName.put("last", "B");
		
		//GenericRecord oGenRec = new GenericData.Record(schema);
		//oGenRec.put("id", 1000);
		//oGenRec.put("name", oRecName);
		
		GenericRecord oGenRec2 = new GenericData.Record(schema);

		oGenRec2.put("id", 1001);
		oGenRec2.put("name", "Barnaby");
		//oGenRec2.put("fullname.last", "J");
		
		try{
			DatumWriter<GenericRecord> userDatumWriter = new SpecificDatumWriter<GenericRecord>(schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(userDatumWriter);

			try{		
				dataFileWriter.create(oGenRec2.getSchema(), file);
				//dataFileWriter.append(oGenRec);
				dataFileWriter.append(oGenRec2);
				System.out.println("Done..");
			}catch(IOException e){
				StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		        StackTraceElement oStk = stacktrace[1]; //method name
				System.out.println("***Exception in method:"+oStk.getMethodName()+"\n****"+e.toString());
			}
			finally{
					dataFileWriter.close();
			}
		}catch(IOException ex2){
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement oStk = stacktrace[1];
			System.out.println("***Exception in method:"+oStk.getMethodName()+"\n***"+ex2.toString());
		}

		System.out.println("Completed. Avro records serialized to disc");
	
	}
	
	public static void main(String[] args){
		try{	
			AvroRecordUsingSchema_AvroArray obj = new AvroRecordUsingSchema_AvroArray();
			
			//obj.generateSchemaFromFile("user.avsc");
			obj.generateSchemaFromText();

			//obj.writeAvroDataToFile("/home/kiran/data_users.avro"); //Giving Error-Need to fix
			
			//obj.readAvroDataFromFile("/home/kiran/data_users.avro"); //Path doesn't work
			
			//obj.processAvroSchemaInMemory("user.avsc");
		}catch(Exception ex2){
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement oStk = stacktrace[1];
			System.out.println("***Exception in method:"+oStk.getMethodName()+"\n***"+ex2.toString());
		}
	}
}