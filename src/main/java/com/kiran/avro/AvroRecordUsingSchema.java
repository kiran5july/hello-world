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

public class AvroRecordUsingSchema {
	
	public Schema.Parser parser = new Schema.Parser();
	public Schema schema;
	
	private void generateSchemaFromFile(String sSchemaFileName) {
		try{
			//parser = new Schema.Parser();
			schema = parser.parse(getClass().getResourceAsStream(sSchemaFileName));
		}catch(Exception e){}
	}
	
	private void generateSchemaFromText() {
		try{
			//parser = new Schema.Parser();
			schema = new Schema.Parser().parse(
					"{" +
					"\"namespace\": \"com.kiran.avro\"," +
					"\"type\": \"record\"," +
					"\"name\": \"User\"," +
					"\"fields\": [" +
					"{\"name\": \"id\", \"type\": \"int\"}," +
					"{\"name\": \"name\", \"type\": \"string\"}," +
					"{\"name\": \"profession\", \"type\": [\"string\", \"null\"]}," +
					"{\"name\": \"email\", \"type\": [\"string\", \"null\"]}," +
					"{\"name\": \"login\", \"type\": [\"string\", \"null\"]}," +
					"{\"name\": \"loc\", \"type\": [\"string\", \"null\"], \"default\":\"null\"}" + //"default" when new element added to schema
					" ]" +
					"}"
				 );
		}catch(Exception e){
			System.out.println("Exception during schema generation:"+ e.toString());
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
				System.out.println("User#:"+(iCounter++)+"->" + oGenRec.get("id")+"/"+oGenRec.get("name")+"/"+oGenRec.get("profession")+"/"+oGenRec.get("email")+"/"+oGenRec.get("login")+"/"+oGenRec.get("loc"));
				
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
		oGenRec.put("name", "Tom");
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
		
		GenericRecord oGenRec = new GenericData.Record(schema);
		oGenRec.put("id", 1000);
		oGenRec.put("name", "Tom");
		oGenRec.put("profession", "SW");
		oGenRec.put("email", "tom@tom.com");

		GenericRecord oGenRec2 = new GenericData.Record(schema);
		oGenRec2.put("id", 1001);
		oGenRec2.put("name", "Barnaby");
		oGenRec2.put("profession", "HR");
		oGenRec2.put("email", "barn@barn.com");
		
		try{
			DatumWriter<GenericRecord> userDatumWriter = new SpecificDatumWriter<GenericRecord>(schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(userDatumWriter);

			try{		
				dataFileWriter.create(oGenRec.getSchema(), file);
				dataFileWriter.append(oGenRec);
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
		
		AvroRecordUsingSchema obj = new AvroRecordUsingSchema();
		
		//obj.generateSchemaFromFile("user.avsc");
		obj.generateSchemaFromText();
		
		obj.readAvroDataFromFile("data_users.avro"); //Path doesn't work
		
		//obj.writeAvroDataToFile("/home/kiran/km/km_hadoop/users_avro_data2.avro");
		
		//obj.processAvroSchemaInMemory("user.avsc");
		
	}
}