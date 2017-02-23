package com.kiran;

import java.io.BufferedReader; 
import java.io.FileReader; 
import java.util.ArrayList; 
import java.util.Collections; 
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer; 
 
public class HashMapEx { 
 
    public static void main(String[] args) { 
        // Creating new HashMap objects 
        // keys are String, values are Integer 
        HashMap<String, Integer> wcHM = new HashMap<String, Integer>();
 
        try { 
            // Opening file
            BufferedReader in = new BufferedReader(new FileReader( "/home/kiran/km/km_hadoop/data/data_wordcount"));
            // string buffer for file reading   
            String sLine;
 
            // reading line by line from file    
            while ((sLine = in.readLine()) != null) { 
            	sLine = sLine.toLowerCase(); // convert to lower case 

                StringTokenizer itr = new StringTokenizer(sLine);
                String word;
                while (itr.hasMoreTokens()) {
                	word = itr.nextToken().toLowerCase();
                	word = word.replaceAll("[^a-zA-Z0-9_-]+","");

                            // Check if word is in HashMap 
                            if (wcHM.containsKey(word)) { 
                                // get number of occurrences for this word & increment count
                            	wcHM.put(word, wcHM.get(word) + 1);
                            } else { 
                                // this is first time we see this word, set value '1' 
                            	wcHM.put(word, 1);
                            } 
                        } 
                    } 

            in.close();
        } catch (Exception e) { 
            e.printStackTrace();
            System.exit(1);
        }
 

        //Print out the data in Map
        Iterator<Map.Entry<String, Integer>> itr = wcHM.entrySet().iterator();
        
        System.out.println("Values in HashMap:");
        System.out.println("------------------");
        while(itr.hasNext()){
        	Map.Entry<String, Integer> me = itr.next();
          System.out.println(me.getKey() + " : " + me.getValue() );
        }

	    //using ArrayList
        System.out.println("Reading using ArrayList...");
        // First we're getting values array  
        ArrayList<Integer> values = new ArrayList<Integer>();
        values.addAll(wcHM.values());
        // and sorting it (in reverse order) 
        System.out.println("Sorting..");
        Collections.sort(values, Collections.reverseOrder());
 
        int last_i = -1;
        // Now, for each value  
        for (Integer oInt : values) { 
            if (last_i == oInt) // without duplicates  
                continue;
            last_i = oInt;
            // we print all hash keys  
            for (String s : wcHM.keySet()) { 
                if (wcHM.get(s) == oInt) // which have this value  
                    System.out.println(s + ":" + oInt);
            } 
            // pretty inefficient, but works  
        } 


    } 
} 