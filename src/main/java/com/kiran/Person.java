package com.kiran;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

public class Person {
	private String fName;
	private String lName;
	private Date dob;
	private String sDateFormat = "yyyy-MM-dd";
	
	public Person(String name, String dob){
		try{
		String sNameSep = "";
		if(name.indexOf(',')>0){
			sNameSep = ",";
		}else{
			sNameSep = " ";
		}
		String[] names = name.split(sNameSep);
		this.lName = names[0];
		this.fName = names[1];
		this.dob=new SimpleDateFormat(sDateFormat).parse(dob);
		
		}catch(ParseException pe){
			System.out.println("Incorrect Date format, it should be in format:"+sDateFormat);
		}catch(Exception ex){
			System.out.println("Exception happened: ");
			ex.printStackTrace();
		}
		
	}
	
	public String printPerson(){
		return "Name:"+this.lName+", "+this.fName+
				", DOB=" + String.format("%1$tY-%1$tm-%1$td", this.dob)+ 
				", Age=" + getAge();
	}
	private String getAge(){
	      int years = 0;
	      int months = 0;
	      int days = 0;
	      //create calendar object for birth day
	      Calendar birthDay = Calendar.getInstance();
	      birthDay.setTimeInMillis(this.dob.getTime());
	      //create calendar object for current day
	      long currentTime = System.currentTimeMillis();
	      Calendar now = Calendar.getInstance();
	      now.setTimeInMillis(currentTime);
	      //Get difference between years
	      years = now.get(Calendar.YEAR) - birthDay.get(Calendar.YEAR);
	      int currMonth = now.get(Calendar.MONTH) + 1;
	      int birthMonth = birthDay.get(Calendar.MONTH) + 1;
	      //Get difference between months
	      months = currMonth - birthMonth;
	      //if month difference is in negative then reduce years by one and calculate the number of months.
	      if (months < 0)
	      {
	         years--;
	         months = 12 - birthMonth + currMonth;
	         if (now.get(Calendar.DATE) < birthDay.get(Calendar.DATE))
	            months--;
	      } else if (months == 0 && now.get(Calendar.DATE) < birthDay.get(Calendar.DATE))
	      {
	         years--;
	         months = 11;
	      }
	      //Calculate the days
	      if (now.get(Calendar.DATE) > birthDay.get(Calendar.DATE))
	         days = now.get(Calendar.DATE) - birthDay.get(Calendar.DATE);
	      else if (now.get(Calendar.DATE) < birthDay.get(Calendar.DATE))
	      {
	         int today = now.get(Calendar.DAY_OF_MONTH);
	         now.add(Calendar.MONTH, -1);
	         days = now.getActualMaximum(Calendar.DAY_OF_MONTH) - birthDay.get(Calendar.DAY_OF_MONTH) + today;
	      } else
	      {
	         days = 0;
	         if (months == 12)
	         {
	            years++;
	            months = 0;
	         }
	      }
	      //Create new Age object 
	      return years+" yrs, "+months+" months, "+days+" days";
	   }

	public static void main(String args[]){
		
		Person persons[] = new Person[3];
		persons[0] = new Person("Duck, Donald", "2000-01-31");
		persons[1] = new Person("Pan Peter", "2001-01-31");
		persons[2] = new Person("Mack, Merry", "2002-02-21");
		
		for(Person p: persons){
			System.out.println(p.printPerson());
			
		}
		
		System.out.println("Array Lists........");
		ArrayList ps = new ArrayList();
		Person p1 = new Person("Arnold, Amy", "1989-11-30");
		ps.add(p1);
		Person p2 = new Person("Beck, Bill", "1999-12-31");
		ps.add(p2);
		int totalElem = ps.size();
		for(int i=0; i<totalElem; i++){
			System.out.println( ((Person) ps.get(i)).printPerson());
		}
		
		System.out.println("Iterator......");
		Iterator itr = ps.iterator();
		while(itr.hasNext())
			System.out.println( ((Person) itr.next()).printPerson());
		
	}
}
