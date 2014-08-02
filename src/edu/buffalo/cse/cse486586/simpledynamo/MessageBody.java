package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

public class MessageBody implements Serializable{

	public String messageType;
	public ArrayList<String> parameters;
	public HashMap<String,String> keyValuePairs;
	public HashMap<String, VersionObject> keyVersionPairs;
	public ArrayList<String> nodeSet;
	public String sender;
	
	public MessageBody(String messageType,ArrayList<String> parameters, HashMap<String,String>values, HashMap<String,VersionObject> versions) 
	{
		this.messageType=messageType;
		this.parameters=parameters;
		if(versions==null)
			versions=new HashMap<String, VersionObject>();
		if(values==null)
			values=new HashMap<String, String>();
		
		this.keyValuePairs=values;
		this.keyVersionPairs=versions;
		
		
	}
}
