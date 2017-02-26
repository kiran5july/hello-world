package com.kiran;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.StringTokenizer;

public class StockQuote {

	static void printStockQuote(String symbol){
		String csvString;
		URL url = null;
		URLConnection urlConn = null;
		InputStreamReader inStream = null;
		BufferedReader buff = null;
		
		try{

			
			url = new URL("http://quote.yahoo.com/d/quotes.csv?s="+symbol+"&f=sl1d1t1c1ohgv&e=.csv");
			
			urlConn = url.openConnection();
			
			inStream = new InputStreamReader(urlConn.getInputStream());
			buff = new BufferedReader(inStream);
			
			//Another way:
			//inStream = url.openStream();
			//buff = new BufferedReader(new InputStreamReader(inStream));
			
			csvString = buff.readLine();
			
			StringTokenizer tokenizer = new StringTokenizer(csvString, ",");
			
			String ticker = tokenizer.nextToken();
			String price = tokenizer.nextToken();
			String tradeDate = tokenizer.nextToken();
			String tradeTime = tokenizer.nextToken();
			float f1 = Float.parseFloat(tokenizer.nextToken());
			float f2 = Float.parseFloat(tokenizer.nextToken());
			float f3 = Float.parseFloat(tokenizer.nextToken());
			float f4 = Float.parseFloat(tokenizer.nextToken());
			long l1= Long.parseLong(tokenizer.nextToken());
			
			System.out.println("Symbol: "+ ticker+
					" Price: "+price+
					" Date: "+tradeDate+
					" Time: "+tradeTime+
					" Growth: " + String.format("%.2f", f1)+
					" Curr: " + String.format("%.2f", f2)+
					" Prev: " + String.format("%.2f", f3)+
					" Prev Day: "+ String.format("%.2f", f4)+
					" Pts: "+l1
					);
			
		}catch(MalformedURLException e){
			System.out.println("Please check URL:"+e.toString());
		}catch(IOException e){
			System.out.println("Can't read from internet: "+e.getMessage());
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
	public static void main(String args[]){
		/*
		if(args.length==0){
			System.out.println("Sample Usage: java StockQuote IBM");
			System.exit(0);
		}
		*/
		
		printStockQuote("IBM");
	}
	
			
}
