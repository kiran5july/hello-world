package com.kiran.mr;

public class EmpRecord
{
	private String sEmpNo;
	private String sEmpName;
	private String sDeptName;
	private double dSal;
	private String sLoc;
	
	public EmpRecord(String sEmpNo, String sEmpName, String sDeptName, double dSal, String  sLoc){
		this.sEmpNo = sEmpNo;
		this.sEmpName = sEmpName;
		this.sDeptName = sDeptName;
		this.dSal=dSal;
		this.sLoc = sLoc;
	}
	public void setEmpNo(String sEmpNo){
		this.sEmpNo = sEmpNo;
	}
	public void setEmpName(String sEmpName){
		this.sEmpName = sEmpName;
	}
	public void setDeptName(String sDeptName){
		this.sDeptName = sDeptName;
	}
	public void setSal(double dSal){
		this.dSal=dSal;
	}
	public void setLoc(String sLoc){
		this.sLoc = sLoc;
	}
	
	public String getEmpNo(){
		return(this.sEmpNo);
	}
	public String getEmpName(){
		return(this.sEmpName);
	}
	public String getDeptName(){
		return(this.sDeptName);
	}
	public double getSal(){
		return(this.dSal);
	}
	public String getLoc(){
		return(this.sLoc);
	}
}