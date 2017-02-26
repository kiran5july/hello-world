package com.kiran;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class WebPageReader {
	
	public static void main(String args[]){
		String nextLine;
		URL url = null;
		URLConnection urlConn = null;
		
		InputStreamReader inStream = null;
		//InputStream inStream = null;
		BufferedReader buff =null;
		
		try{
			url = new URL("http://www.google.com");
			urlConn = url.openConnection();
			
			inStream = new InputStreamReader(urlConn.getInputStream(), "UTF8");
			buff = new BufferedReader(inStream);
			
			//Another way:
			//inStream = url.openStream();
			//buff = new BufferedReader(new InputStreamReader(inStream));
			
			//read & print
			while(true)  //(nextLine = buff.readLine()) != null)
			{
				nextLine = buff.readLine();
				if(nextLine != null){
					System.out.println(nextLine);
				}else
					break;
			}
		}catch(MalformedURLException e){
			System.out.println("Please check URL:"+e.toString());
		}catch(IOException e){
			System.out.println("Can't read from internet: "+e.toString());
		}finally{
			if(inStream != null){
				try{
					inStream.close();
					buff.close();
				}catch(IOException e){
					System.out.println("Can't close the stream:"+e.toString());
				}
			}
		}
	}
}
