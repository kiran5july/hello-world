package com.kiran.avro;
import java.io.File;
import java.io.IOException;

//import org.apache.avro.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroSerDeUsingObject
{

	public void readAvroObjFromFile(String sAvroDataFileName){
		
		File sAvroDataFile = new File(sAvroDataFileName);
		
		// DeSerialize user1, user2 and user3 from disk/file
		try{
			// Deserialize Users from disk
			System.out.println("Reading Avro records from disc/file......");
			DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
			DataFileReader<User> dataFileReader = new DataFileReader<User>(sAvroDataFile, userDatumReader);
			try{
			User user = null;
			int iCounter = 0;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println("User#:"+(iCounter++)+":" + user.getId()+"/"+user.getName()+"/"+user.getProfession()+"/"+user.getEmail()+"/"+user.getLogin()+"/"+user.getLoc());
				
			}
			}catch(IOException e){
				StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		        StackTraceElement oStk = stacktrace[1];
				System.out.println("Exception in method:"+oStk.getMethodName()+"\n"+e.toString());
			}
			finally{
				dataFileReader.close();
			}
		}catch(IOException e){
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement oStk = stacktrace[1];
			System.out.println("***Exception in method:"+oStk.getMethodName()+"\n****"+e.toString());
		}finally{
			
		}
	}
	
	public void writeAvroObjToFile(String sAvroDataFileName){
		
		User user1 = new User();
		user1.setId(1001);
		user1.setName("Alyssa");
		//user1.setProfession("Accountant"); //leave it null
		user1.setEmail("alys@gmail.com");
		user1.setLogin("alys1");
		user1.setLoc("Los Angeles");
		System.out.println("Avro User1:"+user1.getId()+"/"+user1.getName()+"/"+user1.getProfession()+"/"+user1.getEmail()+"/"+user1.getLogin()+"/"+user1.getLoc());
		
		// Alternate constructor
		User user2 = new User(1002, "Ben", "HR", null, "ben1","Seattle");
		System.out.println("Avro User2:"+user2.getId()+"/"+user2.getName()+"/"+user2.getProfession()+"/"+user2.getEmail()+"/"+user2.getLogin()+"/"+user2.getLoc());
		
		// Construct via builder
		User user3 = User.newBuilder()
					.setId(1003)
		             .setName("Charlie")
		             .setProfession(null)
		             .setEmail("charlie@gmail.com")
		             .setLogin("charlie1")
		             .setLoc(null)
		             .build();
		System.out.println("Avro User3:"+user3.getId()+"/"+user3.getName()+"/"+user3.getProfession()+"/"+user3.getEmail()+"/"+user3.getLogin()+"/"+user3.getLoc());
	
		
		// Serialize user1, user2 and user3 to disk/file & read from it
		File sAvroDataFile = new File(sAvroDataFileName);
		
		try{
			DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
			DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);

			try{		
				dataFileWriter.create(user1.getSchema(), sAvroDataFile);
				dataFileWriter.append(user1);
				dataFileWriter.append(user2);
				dataFileWriter.append(user3);
			}catch(IOException e){
				StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		        StackTraceElement oStk = stacktrace[1];
				System.out.println("***Exception in method:"+oStk.getMethodName()+"\n***"+e.toString());
			}
			finally{
					dataFileWriter.close();
			}
		}catch(IOException ex2){
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
	        StackTraceElement oStk = stacktrace[1];
			System.out.println("***Exception in method:"+oStk.getMethodName()+"\n****"+ex2.toString());
		}

		System.out.println("Completed. Avro records serialized to disc");
	}
	
	public static void main(String[] args){
		
		String sAvroDataFileName = "/home/kiran/km/km_hadoop/users_avro_data.avro";
		
		AvroSerDeUsingObject obj = new AvroSerDeUsingObject();
		//obj.writeAvroObjToFile(sAvroDataFileName);
		obj.readAvroObjFromFile(sAvroDataFileName);
	
		
		
	}

}