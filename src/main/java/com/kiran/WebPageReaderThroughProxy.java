package com.kiran;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.Proxy;

public class WebPageReaderThroughProxy {
	
	public static void main(String args[]){
		String nextLine;
		URL url = null;
		URLConnection urlConn = null;
		
		InputStreamReader inStream = null;
		BufferedReader buff =null;
		
		try{
			System.setProperty("http.proxyHost", "http://proxy.mycompany.com");
			System.setProperty("http.proxyPort", "8080");
			
			//ANother option using Proxy class
			Proxy myProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("http://proxy.mycompany.com", 8080));
			
			url = new URL("http://www.google.com");
			urlConn = url.openConnection(myProxy);
			
			inStream = new InputStreamReader(urlConn.getInputStream(), "UTF8");
			buff = new BufferedReader(inStream);
			
		
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
