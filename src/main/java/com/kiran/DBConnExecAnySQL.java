package com.kiran;
import java.sql.*;

import com.kiran.mr.WordCountByKey;

public class DBConnExecAnySQL {


  public static void main(String args[]) {
	  System.out.println("args:" + args.length);
	  if(args.length != 3){
		  System.err.printf("KM USAGE ERROR: %s <userName> <password> <SQL>\n", DBConnExecAnySQL.class);
			//ToolRunner.printGenericCommandUsage(System.err);
		  System.exit(1);
		  //return;
	  }
		  
	  String sUser = args[0];
	  String sPwd = args[1];
	  String oSQL = args[2];
	  
	  Connection conn=null;
	  Statement stmt=null;
	  ResultSet rs=null;


	  try {
		  // Class.forName("org.apache.derby.jdbc.ClientDriver");
		  //conn = DriverManager.getConnection( "jdbc:derby://localhost:1527/Lesson22"); 
		  Class.forName("com.mysql.jdbc.Driver");
		  conn = DriverManager.getConnection( "jdbc:mysql://localhost:3306/retail_db", sUser, sPwd);

		  // Create a Statement object
		  stmt = conn.createStatement(); 

		  // Execute SQL Select 
		  rs = stmt.executeQuery(oSQL);  

		  // Find out the number of columns, their names and display the data
		  ResultSetMetaData rsMeta = rs.getMetaData();
		  int colCount = rsMeta.getColumnCount();

		  for (int i = 1; i <= colCount; i++)  {
			  System.out.print(rsMeta.getColumnName(i) + " "); 
		  }
		  System.out.println();
   
		  while (rs.next()){
			  System.out.print("[");
			  for (int i = 1; i <= colCount; i++)  {
				  System.out.print(rs.getString(i) + " "); 
			  }
			  System.out.print("]\n");   // new line character
		  }

	  } catch( SQLException se ) {
		  System.out.println ("SQLError: " + se.getMessage () + " code: " + se.getErrorCode ());

	  } catch( Exception e ) {
		  System.out.println(e.getMessage()); 
		  e.printStackTrace(); 
	  } finally{
		  // clean up the system resources
		  try{
			  rs.close();     
			  stmt.close(); 
			  conn.close();  
		  } catch(Exception e){
			  e.printStackTrace();
		  } 
	  }
  }
}