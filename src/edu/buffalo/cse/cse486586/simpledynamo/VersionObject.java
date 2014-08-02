package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class VersionObject implements Serializable {
	public int versionNumber;
	public String value;
	public VersionObject(int number,String str)
	{
		versionNumber=number;
		value=str;
	}
}
