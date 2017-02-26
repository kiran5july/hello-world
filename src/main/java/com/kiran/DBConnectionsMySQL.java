package com.kiran;

import java.sql.*; 

public class DBConnectionsMySQL {

	  public static void main(String args[]) {
		   Connection conn=null;
		   Statement stmt=null;
		   ResultSet rs=null;
		   

		   try {
		    // Load the JDBC driver  
			// This can be skipped for Derby, but derbyclient.jar has to be in the CLASSPATH   
		    // Class.forName("org.apache.derby.jdbc.ClientDriver");
			
			Class.forName("com.mysql.jdbc.Driver");
		    conn = DriverManager.getConnection( "jdbc:mysql://localhost:3306/retail_db","km","Kiran$5july@123"); 

		    // Build an SQL String 
		    String sqlQuery = "SELECT * from products"; 
				
		    // Create a Statement object
		    stmt = conn.createStatement(); 

		    // Execute SQL and get obtain the ResultSet object
		    rs = stmt.executeQuery(sqlQuery);  

		    // Process the result set - print Employees
		    while (rs.next()){ 
		    	 int iProdId = rs.getInt("product_id");
		       	 String iProdCatId = rs.getString("product_category_id");
		         String sName = rs.getString("product_name");
		         String sProdDesc = rs.getString("product_description");
		         Float fPrice = rs.getFloat("product_price");
		         String sProdImg = rs.getString("product_image");
		         
			     System.out.println("["+ iProdId + ", " + iProdCatId + ", " + sName  + ", " + sProdDesc + ", " + fPrice + ", " + sProdImg + "]"); 
		    }

		   } catch( SQLException se ) {
		      System.out.println ("SQLError: " + se.getMessage ()
		           + " code: " + se.getErrorCode ());

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
