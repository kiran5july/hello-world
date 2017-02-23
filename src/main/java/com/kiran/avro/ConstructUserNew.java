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

public class ConstructUserNew
{

	public static void main(String[] args){
	
		UserNew user1 = new UserNew();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null
		System.out.println("Avro User1:"+user1.getName()+"/"+user1.getFavoriteColor()+"/"+user1.getFavoriteNumber());
		
		// Alternate constructor
		UserNew user2 = new UserNew("Ben", 7, "red");
		System.out.println("Avro User2:"+user2.getName()+"/"+user2.getFavoriteColor()+"/"+user2.getFavoriteNumber());
		
		// Construct via builder
		UserNew user3 = UserNew.newBuilder()
		             .setName("Charlie")
		             .setFavoriteColor("blue")
		             .setFavoriteNumber(null)
		             .build();
		System.out.println("Avro User3:"+user3.getName()+"/"+user3.getFavoriteColor()+"/"+user3.getFavoriteNumber());
	
		
		// Serialize user1, user2 and user3 to disk
		File file = new File("/home/kiran/km/km_hadoop/users_avro_data.avro");
		try{
			DatumWriter<UserNew> userDatumWriter = new SpecificDatumWriter<UserNew>(UserNew.class);
			DataFileWriter<UserNew> dataFileWriter = new DataFileWriter<UserNew>(userDatumWriter);

			try{		
				dataFileWriter.create(user1.getSchema(), file);
				dataFileWriter.append(user1);
				dataFileWriter.append(user2);
				dataFileWriter.append(user3);
			}catch(IOException e){
			}
			finally{
					dataFileWriter.close();
			}
		}catch(IOException ex2){
			
		}

		System.out.println("Completed. Avro records serialized to disc");
		
		
		try{
			// Deserialize Users from disk
			System.out.println("Reading Avro records from disc......");
			DatumReader<UserNew> userDatumReader = new SpecificDatumReader<UserNew>(UserNew.class);
			DataFileReader<UserNew> dataFileReader = new DataFileReader<UserNew>(file, userDatumReader);
			try{
			UserNew user = null;
			int iCounter = 0;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println("User#"+(iCounter++)+user);
				
			}
			}catch(IOException e){}
			finally{
				dataFileReader.close();
			}
		}catch(IOException e){
			
		}finally{
			
		}
		
		
		
	}

}