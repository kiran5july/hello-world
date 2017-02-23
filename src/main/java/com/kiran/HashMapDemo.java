package com.kiran;

import java.util.*;

public class HashMapDemo {

  public static void main(String[] args) {

	HashMap<String, Double> hm = new HashMap<String, Double>();
    
    hm.put("Rohit", new Double(3434.34));
    hm.put("Mohit", new Double(123.22));
    hm.put("Ashish", new Double(1200.34));
    hm.put("Scott", new Double(999.34));
    hm.put("Pankaj", new Double(-19.34));
    
    
    System.out.println("Values in HashMap:");
    System.out.println("------------------");
    Iterator<Map.Entry<String, Double>> itr = hm.entrySet().iterator();
    while(itr.hasNext()){
      Map.Entry<String, Double> me = itr.next();
      System.out.println(me.getKey() + " : " + me.getValue() );
    }
    System.out.println("------------------");
    
    //deposit into Rohit's Account
    System.out.println("Adding 1000 to Rohit's amount..");
    double balance = ((Double)hm.get("Rohit")).doubleValue();
    hm.put("Rohit", new Double(balance + 1000));

    System.out.println("Rohit's new balance : " + hm.get("Rohit"));

  }
} 
