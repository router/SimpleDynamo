package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	
	//Members
	private String myPortStr;
    private int myPort;
    public int AVDNumber;
    private String currentNodeHash;
    public final int SERVER_PORT=10000;
    public final String[] ports={"11108","11112","11116","11120","11124"};
    public final String[] nodeSet={"5562","5556","5554","5558","5560"};
	public final int nodeCount=5;
    private static final String AUTHORITY="edu.buffalo.cse.cse486_586.simpledht.provider";
	public static Uri CONTENT_URI=Uri.parse("content://edu.buffalo.cse.cse486_586.simpledht.provider");
	
	//Members for chord ring information
	private String predecessorHash,successorHash;
    int predecessor, successor; 
    //MessageBody incomingMessage;
    public  String TAG="SimpleDynamo";
    
	String largestInRing, smallestInRing;
	String RequestingAVD; // requesting avd
	
	MatrixCursor outputCursor;
	HashMap<String, VersionObject> outputMap;
	int finalDeleteResult;
	
	//List<String> pendingInserts;
	Map<String,Integer> pendingInserts;
	Map<String,Integer> pendingQueries;
	Map<String,Integer> pendingDeletes;
	
	Map<String, VersionObject> versions;
	
//	boolean isStillSearching=false;
	//boolean isStillDeleting=false;
	boolean isDataSetInitialized=false;
	//String keySearched, valueObtained;
	boolean firstTime;
	int recoveryStatus;
	boolean isNeighbourActive;
	int insertc,insertr;
	int queryc,queryr;
	boolean hasCopied;
	public int delete(String selection)
	{
		//while(recoveryStatus>0);
		File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),selection);
		f.delete();
		
		return 1;
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		finalDeleteResult=0;
		//while(recoveryStatus>0);
		boolean returnStatus=true;
    	if(selection.equals("@"))
    	{
    		File[] files=this.getContext().getFilesDir().listFiles();
    		for(int i=0;i<files.length;i++)
    		{

    			if(selection.equals("logfile"))
    				continue;
    			
    			returnStatus=returnStatus&&files[i].delete();
    			
    		}
    		return (returnStatus==true)?0:1;
    	}
    	else if(selection.equals("*"))
    	{
    		
    		File[] files=this.getContext().getFilesDir().listFiles();
    		for(int i=0;i<files.length;i++)
    		{
    			File f=files[i];
    			selection = f.getName();
    			if(selection.equals("logfile"))
    				continue;
    			returnStatus=returnStatus&&files[i].delete();
                
                       			
    		}
    		if(!currentNodeHash.equals(predecessorHash)) // more than 2 items on the ring
    		{
        		ArrayList<String>param=new ArrayList<String>();
        		param.add("*");
        		param.add(String.valueOf(AVDNumber));
        		param.add(String.valueOf(returnStatus==true?1:0));
        		new ClientThread(new MessageBody("DeleteKey",param,null,null),String.valueOf(successor*2)).start();
        		//isStillDeleting=true;
        		
        		//pendingDeletes.add("*");
        		handleRequest("*", "add",pendingDeletes);
        		while(pendingDeletes.containsKey("*"));
    		}
    		else //single node , return result
    		{
    			return (returnStatus==true)?0:1;
    		}
    			
    	}
    	else // single file
    	{
    		
    		int coordinator=Integer.parseInt(nodeForKey(selection));
    		
    		if(coordinator==AVDNumber)
    		{
    			File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),selection);
    			f.delete();
    			
    		}
    		else
    		{
    			ArrayList<String>param=new ArrayList<String>();
        		param.add(selection);
        		param.add(String.valueOf(AVDNumber));
        		new ClientThread(new MessageBody("DeleteKey",param,null,null),String.valueOf(coordinator*2)).start();
        		//isStillDeleting=true;
        		
        		handleRequest(selection,"add", pendingDeletes);
    		}
    		
			ArrayList<String>param=new ArrayList<String>();
    		param.add(selection);
    		param.add(String.valueOf(AVDNumber));
    		
    		String succ=successorForNode(String.valueOf(coordinator));
        	//logToFile("Sending delete "+selection+" to replica: "+succ);
        	int destPort=2*Integer.parseInt(succ);
        	MessageBody deleteMessage=new MessageBody("DeleteKey", param, null,null);
        	//SimpleDynamoProvider.this.logToFile(deleteMessage);
        	new ClientThread(deleteMessage, String.valueOf(destPort)).start();
        	String key=new String(selection);
        	handleRequest(key, "add", pendingDeletes);
        	
        	succ=successorForNode(succ);
        	//logToFile("Sending "+selection+" to replica: "+succ);
        	destPort=2*Integer.parseInt(succ);
        	deleteMessage=new MessageBody("DeleteKey", param, null,null);
        	//SimpleDynamoProvider.this.logToFile(deleteMessage);
        	new ClientThread(deleteMessage, String.valueOf(destPort)).start();
        	handleRequest(key, "add", pendingDeletes);

        	while(pendingDeletes.containsKey(selection) && pendingDeletes.get(selection)>1);
        	logToFile("RETURNING FROM DELETE"+selection);
    	}
    	
    	
    	return 1;//finalDeleteResult;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void insert(String key,VersionObject obj)
	{
		try
		{
			File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),key);
			String value="";
			if(f.exists())
			{
				FileInputStream inputStream=new FileInputStream(f);
		        
	            BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
	            String line="";
	            while((line=buf.readLine())!=null)
	            	value+=line;
	            
	            buf.close();
	            inputStream.close();
	            value=value.trim();
				
			}
			int versionNumber=obj.versionNumber;
			String finalValue=obj.value;
			if(value.length()>0)
			{
				String[] comp=value.split(":");
				versionNumber+=Integer.parseInt(comp[0]);
				finalValue=comp[1];
			}

			String writevalue=String.valueOf(versionNumber)+":"+finalValue;
			FileOutputStream outputStream = new FileOutputStream(f);//openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(writevalue.getBytes());
            outputStream.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	 @Override
	public Uri insert(Uri uri, ContentValues values) {
		
		 	// TODO Auto-generated method stub
		 	//while(recoveryStatus>0);
		 	
		 	logToFile("INSERTCALLED:"+String.valueOf(++insertc));
	        Log.v("insert", values.toString());
	        String filename=values.getAsString("key");
	        String value=values.getAsString("value");
	        int repCount=2; // for first time call the value should be 2 
	        if(values.containsKey("repCount"))
	        	repCount=Integer.parseInt(values.getAsString("repCount"));
	        
	        if(firstTime)
	        {
	        	firstTime=false;
	        	String logStr="predhash: "+predecessorHash+" succHash: "+successorHash+"largest: "+largestInRing+"smallest: "+smallestInRing+" pred:"+String.valueOf(predecessor)+" succ:"+String.valueOf(successor);
	        	SimpleDynamoProvider.this.logToFile(logStr);
	        }
	        
	        CONTENT_URI=uri;
	        
	        String coordinator=nodeForKey(filename);
	        
	        logToFile("key: "+filename+" node:"+coordinator+"repcount"+String.valueOf(repCount));
//	        if(nodeForKey.equals(String.valueOf(AVDNumber))|| repCount<2)
//	        {
//	        	try 
//				{
//	        		File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),filename);
//	            	
//	            	if(!f.exists())
//	            		f.createNewFile();
//	            	FileOutputStream outputStream = new FileOutputStream(f);//openFileOutput(filename, Context.MODE_PRIVATE);
//	                outputStream.write(value.getBytes());
//	                outputStream.close();
//				} 
//				catch (Exception e)
//				{
//					logToFile("Unable to write\n");
//					Log.e("SimpleDhtActivity", "File write failed");
//				}
//	        	
//	        	if(repCount>0)
//	        	{
//	        		logToFile("Sending "+filename+" to next: "+String.valueOf(successor));
//	        		ArrayList<String> param=new ArrayList<String>();
//		        	param.add(filename);
//		        	param.add(value);
//		        	param.add(String.valueOf(repCount-1));
//		        	Log.d(TAG,"key "+filename+ "going over the n/w" );
//		        	int succPort=2*successor;
//		        	
//		        	MessageBody insertMessage=new MessageBody("InsertKeyReplica", param, null);
//		        	SimpleDynamoProvider.this.logToFile(insertMessage);
//		        	new ClientThread(insertMessage, String.valueOf(succPort)).start();
//		        	//if(pendingInserts.contains(filename)) // case of original sender being a replica
//		        		//pendingInserts.remove(filename);
//	        	}
//	        	
//	        	
//	        }
//	        else //send message to the key's coordinator
//	        {
//	        	logToFile("Sending "+filename+" to coord: "+nodeForKey);
//	        	ArrayList<String> param=new ArrayList<String>();
//	        	param.add(String.valueOf(AVDNumber));
//	        	param.add(filename);
//	        	param.add(value);
//	        	param.add(String.valueOf(repCount));
//	        	Log.d(TAG,"key "+filename+ "going over the n/w" );
//	        	int succPort=2*Integer.parseInt(nodeForKey);
//	        	
//	        	logToFile("sending to port "+String.valueOf(succPort)+"----->");
//	        	MessageBody insertMessage=new MessageBody("InsertKeyCoord", param, null);
//	        	SimpleDynamoProvider.this.logToFile(insertMessage);
//	        	new ClientThread(insertMessage, String.valueOf(succPort)).start();
//	        	String key=new String(filename);
//	        	pendingInserts.add(key);
//
//
//		        while(pendingInserts.containsKey(key));
//	        }	
	        
	        if(coordinator.equals(String.valueOf(AVDNumber))|| repCount==0) //current node is the coordinator or it is one of the replicas / coord that needs to be written
	        {
	        	// Check for version, increase by 1 and update to file
	        	try 
				{
	        		File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),filename);
	        		int versionNumber=-1;
	            	if(!f.exists())
	            	{
	            		f.createNewFile();
	            		versionNumber=0;
	            	}
	            	else
	            	{
	            		//get old version
	            	
	        			FileInputStream inputStream=new FileInputStream(f);
	                    BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
	                    String valueread="",line="";
	                    while((line=buf.readLine())!=null)
	                    	valueread+=line;
	                    
	                    buf.close();
	                    
	                    valueread=valueread.trim();
	                    String[] comp=valueread.split(":");
	                    versionNumber=Integer.parseInt(comp[0]);
	                    inputStream.close();
	            	}
	            	
                    versionNumber++;
                    String writevalue=String.valueOf(versionNumber)+":"+value;
                    
                    FileOutputStream outputStream = new FileOutputStream(f);//openFileOutput(filename, Context.MODE_PRIVATE);
	                outputStream.write(writevalue.getBytes());
	                outputStream.close();
	                
				} 
				catch (Exception e)
				{
					logToFile("Unable to write\n");
					Log.e("SimpleDhtActivity", "File write failed");
				}
	        	
	        }
	        else // send message to coord
	        {
	        	logToFile("Sending "+filename+" to coord: "+coordinator);
	        	ArrayList<String> param=new ArrayList<String>();
	        	param.add(String.valueOf(AVDNumber));
	        	param.add(filename);
	        	param.add(value);
	        	param.add("0");
	        	Log.d(TAG,"key "+filename+ "going over the n/w" );
	        	int coordPort=2*Integer.parseInt(coordinator);
	        	
	        	//logToFile("sending to port "+String.valueOf(coordPort)+"----->");
	        	MessageBody insertMessage=new MessageBody("InsertKey", param, null,null);
	        	//SimpleDynamoProvider.this.logToFile(insertMessage);
	        	new ClientThread(insertMessage, String.valueOf(coordPort)).start();
	        	String key=new String(filename);
	        	//pendingInserts.add(key);
	        	handleRequest(key, "add", pendingInserts);
	        	
	        }
	        
	        if(repCount==2) // for the original caller only
	        {
		        //send to the 2 replicas
	        	
	        	ArrayList<String> param=new ArrayList<String>();
	        	param.add(String.valueOf(AVDNumber));
	        	param.add(filename);
	        	param.add(value);
	        	param.add("0");
	        	
	        	
	        	String succ=successorForNode(coordinator);
	        	logToFile("Sending "+filename+" to replica: "+succ);
	        	int destPort=2*Integer.parseInt(succ);
	        	MessageBody insertMessage=new MessageBody("InsertKey", param, null,null);
	        	//SimpleDynamoProvider.this.logToFile(insertMessage);
	        	new ClientThread(insertMessage, String.valueOf(destPort)).start();
	        	String key=new String(filename);
	        	handleRequest(key, "add", pendingInserts);
	        	
	        	succ=successorForNode(succ);
	        	logToFile("Sending "+filename+" to replica: "+succ);
	        	destPort=2*Integer.parseInt(succ);
	        	insertMessage=new MessageBody("InsertKey", param, null,null);
	        	//SimpleDynamoProvider.this.logToFile(insertMessage);
	        	new ClientThread(insertMessage, String.valueOf(destPort)).start();
	        	handleRequest(key, "add", pendingInserts);

	        	while(pendingInserts.containsKey(key) && pendingInserts.get(key)>1);
	        	 
	        }

	        logToFile("INSERT RETURNED:"+String.valueOf(++insertr));
	        logToFile("Insert returning "+filename);
	        
	        
	       
        	return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		
		//Flush the contents of the persistent storage . The following code needs to be commented since on relaunch after failure, we need the old data to build the merkel trees to compare.
		insertc=insertr=0;
		queryc=queryr=0;
   		String[] cols={"key","value"};
		outputCursor=new MatrixCursor(cols);
    	File rootDir=this.getContext().getFilesDir();
    	if(rootDir.isDirectory()) {
            String[] children = rootDir.list();
            for (int i = 0; i < children.length; i++) {
            	if(children[i].equals("logfile"))
            		continue;
            	
                new File(rootDir, children[i]).delete();
            }
    	}
    	
    	recoveryStatus=0;

    
    	
    	
//    	pendingInserts=Collections.synchronizedMap(new HashMap<String,Integer>());//Collections.synchronizedList(new ArrayList<String>());
//    	pendingDeletes=Collections.synchronizedMap(new HashMap<String,Integer>());//new ArrayList<String>();
//    	pendingQueries=Collections.synchronizedMap(new HashMap<String,Integer>());//new ArrayList<String>();
//    	versions=Collections.synchronizedMap(new HashMap<String,VersionObject>());
    	
    	
    	pendingInserts=new HashMap<String,Integer>();//Collections.synchronizedMap();//Collections.synchronizedList(new ArrayList<String>());
    	pendingDeletes=new HashMap<String,Integer>();//Collections.synchronizedMap(new HashMap<String,Integer>());//new ArrayList<String>();
    	pendingQueries=new HashMap<String,Integer>();//Collections.synchronizedMap(new HashMap<String,Integer>());//new ArrayList<String>();
    	versions=new HashMap<String,VersionObject>();//Collections.synchronizedMap(new HashMap<String,VersionObject>());
    	firstTime=true;
    	//logToFile(new MessageBody("logtest", null, null,null)); 
    
    	getAVDDetails();
    	TAG=TAG+String.valueOf(AVDNumber);
        //setup the sever
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
        	//Log.d("abcd", "Can't create a ServerSocket");
        	logToFile("Unable to create socket");
            return false;
        }
    	
    	try
    	{
    		currentNodeHash=genHash(String.valueOf(AVDNumber));
    	}
    	catch(NoSuchAlgorithmException e)
    	{
    		e.printStackTrace();
    	}
    	
    	setupRing();
    	if(isRecoveryNeeded())
    	{
    		//recoveryStatus=2;
    		activateRecovery();
    		
    	}
		return false;
	}

	
	public HashMap<String, VersionObject> query(String selection)
	{
		
		if(selection.equals("@v"))	//get all keys/values with versions 
    	{
    		
			HashMap<String, VersionObject>allFilesDump=new HashMap<String, VersionObject>();
			try 
			{
				File[] files=this.getContext().getFilesDir().listFiles();
	    		for(int i=0;i<files.length;i++)
	    		{
	    			File f=files[i];
	    			selection = f.getName();
	    			FileInputStream inputStream=new FileInputStream(f);
	    			if(f.getName().equals("logfile"))
	    				continue;
	                BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
	                String value="",line="";
	                while((line=buf.readLine())!=null)
	                	value+=line;
	                
	                buf.close();
	                inputStream.close();
	                value=value.trim();
	                String[] comp=value.split(":");
	                VersionObject vOb=new VersionObject(Integer.parseInt(comp[0]),comp[1]);
	                allFilesDump.put(selection,vOb);//	outputCursor=result;
	                
	    		}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
    		return allFilesDump;
    		
    	}
		else // single key lookup
		{
			//while(recoveryStatus>0);
			try 
			{
				
				File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),selection);
				String value="";
				if(f.exists())
				{
					FileInputStream inputStream=new FileInputStream(f);
		        
		            BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
		            String line="";
		            while((line=buf.readLine())!=null)
		            	value+=line;
		            
		            buf.close();
		            inputStream.close();
		            value=value.trim();
				}
				else
					value="-1:##";
	            String[] comp=value.split(":");
	            HashMap<String, VersionObject> result=new HashMap<String, VersionObject>();
	            result.put(selection,new VersionObject(Integer.parseInt(comp[0]),comp[1]));
	            
	            return result;
//	            String[] row={selection,value};
//	            
//	            result.addRow(row);
//	            return result;
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
    		
		}
		return null;
	}
	
	
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        Log.v("query", selection);
        logToFile("QUERYCALLED:"+String.valueOf(++queryc));

        //while(recoveryStatus>0);
        try {

            String[] cols={"key","value"};
            
           // MatrixCursor result=new MatrixCursor(cols);
        	if(selection.equals("@"))	// in case of * query or incase of a local @ query
        	{
        		
        		MatrixCursor result=new MatrixCursor(cols);
        		File[] files=this.getContext().getFilesDir().listFiles();
        		for(int i=0;i<files.length;i++)
        		{
        			File f=files[i];
        			selection = f.getName();
        			FileInputStream inputStream=new FileInputStream(f);
        			if(f.getName().equals("logfile"))
        				continue;
                    BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
                    String value="",line="";
                    while((line=buf.readLine())!=null)
                    	value+=line;
                    
                    buf.close();
                    inputStream.close();
                    value=value.trim();
                    value=value.split(":")[1];
                    String[] row={selection,value};
                    result.addRow(row);
                   //	outputCursor=result;
                    
        		}
        		return result;
        		
        	}
        	
        	else if(selection.equals("*"))
        	{
        		logToFile("looking for *");
        		HashMap<String , String> localOutput=new HashMap<String, String>();
        		File[] files=this.getContext().getFilesDir().listFiles();
        		for(int i=0;i<files.length;i++)
        		{
        			File f=files[i];
        			selection = f.getName();
        			if(selection.equals("logfile"))
        				continue;
        			FileInputStream inputStream=new FileInputStream(f);
                    
                    BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
                    String value="",line="";
                    while((line=buf.readLine())!=null)
                    	value+=line;
                    value=value.trim();
                    value=value.split(":")[1];
                    buf.close();
                    
                    inputStream.close();
                    localOutput.put(selection, value);
                           			
        		}
        		
        		if(!currentNodeHash.equals(predecessorHash)) // more than 2 items on the ring
        		{
        			logToFile("sending over n/w");
	        		ArrayList<String>param=new ArrayList<String>();
	        		param.add("*");
	        		param.add(String.valueOf(AVDNumber));
	        		
	        		
	        		
	        		int tempsucc=successor;
	        		if(!isNeighbourActive())
	        		{
	        			tempsucc=Integer.parseInt(successorForNode(String.valueOf(successor)));
	        		}
	        		new ClientThread(new MessageBody("SearchKey",param,localOutput,null),String.valueOf(tempsucc*2)).start();
	        		//isStillSearching=true;
	        		//synchronized (this) {
		        	handleRequest("*","add",pendingQueries);
		        	while(pendingQueries.containsKey("*") && pendingQueries.get("*")>0);
		        	logToFile("QUERYRETURNED:"+String.valueOf(++queryr));
		        	logToFile("DONE HERE .. RETURNING"+outputCursor.getCount());
		        	return outputCursor;
					//}

        		}
//        		else // hashmap to cursor conversion //only one node in the system // SHOULD NEVER HAPPEN
//        		{
//        			MatrixCursor result=new MatrixCursor(cols);
//	   				 Iterator<String>it =localOutput.keySet().iterator();
//	   				 while(it.hasNext())
//	   				 {
//	   					 String[] value=new String[2];
//	   					 value[0]=it.next();
//	   					 value[1]=localOutput.get(value[0]);
//	   					 value[1]=value[1].trim();
//	   					 result.addRow(value);
//	   					 
//	   				 }
//	   				 return result;
//        		}
//        			
        	}
        	else // single file searched for
        	{
        		
        		int coordinator=Integer.parseInt(nodeForKey(selection));
        		//
            	if(coordinator!=AVDNumber) 
            	{
            		// Get node for key
            		//Log.d(TAG,"File not found"+f.toString());
            		ArrayList<String>param=new ArrayList<String>();
            		param.add(selection);
            		param.add(String.valueOf(AVDNumber));
            		new ClientThread(new MessageBody("SearchKey",param,null,null),String.valueOf(coordinator*2)).start();
            		//isStillSearching=true;
            		String key=new String(selection);
            		handleRequest(key, "add",pendingQueries);
            	}
            	else
            	{
            		logToFile("coord=self");
            		MatrixCursor result=new MatrixCursor(cols);
            		File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),selection);
            		FileInputStream inputStream=new FileInputStream(f);
	            
	                BufferedReader buf= new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
	                String value="",line="";
	                while((line=buf.readLine())!=null)
	                	value+=line;
	                
	                buf.close();
	                inputStream.close();
	                value=value.trim();
	                String[] comp=value.split(":");
	                
	                updateVersions(selection, new VersionObject(Integer.parseInt(comp[0]),comp[1]));
//	                String[] row={selection,value};
//	                
//	                result.addRow(row);
//	                return result;
            	}
            	
            	//send to the 2 replicas
            	
            	ArrayList<String> param=new ArrayList<String>();
            	param.add(selection);
            	param.add(String.valueOf(AVDNumber));
            	
            	String succ=successorForNode(String.valueOf(coordinator));
            	logToFile("querying "+selection+" from replica: "+succ);
            	int destPort=2*Integer.parseInt(succ);
            	MessageBody queryMessage=new MessageBody("SearchKey", param, null,null);
            	SimpleDynamoProvider.this.logToFile(queryMessage);
            	new ClientThread(queryMessage, String.valueOf(destPort)).start();
            	handleRequest(selection, "add", pendingQueries);
            	
            	succ=successorForNode(succ);
            	logToFile("querying "+selection+" from replica: "+succ);
            	destPort=2*Integer.parseInt(succ);
            	queryMessage=new MessageBody("SearchKey", param, null,null);
            	SimpleDynamoProvider.this.logToFile(queryMessage);
            	new ClientThread(queryMessage, String.valueOf(destPort)).start();
            	handleRequest(selection, "add", pendingQueries);

        		while(pendingQueries.containsKey(selection) && pendingQueries.get(selection) > 1 );
        		
        		HashMap<String, VersionObject> finalMap=new HashMap<String, VersionObject>(outputMap);
        		hasCopied=true;
        			
        		MatrixCursor finalOutput=new MatrixCursor(cols);
           		String[] value=new String[2];
      			 value[0]=selection;
      			 value[1]=versions.get(selection).value;
      			finalOutput.addRow(value);
        		//logToFile("QUERYRETURNED:"+String.valueOf(++queryr)+"RETURNING "+selection);
        		return finalOutput;
        	}
        	//synchronized (SimpleDhtProvider.class) {
				
			//}
    		    
	            
        } catch (Exception e) {
            Log.e("SimpleDynamoProvider", "Unable to read file.");
            logToFile("query: exception ---->"+e.getClass());
        };
        
        logToFile("RETURNING EMPTY CURSR: "+selection);
        return null;// this.query(uri, projection, selection, selectionArgs, sortOrder);
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    private  void handleSearchResultSingle(String key,HashMap<String,VersionObject>result)
    {
    	
    	if(pendingQueries.get(key)==2)
    	{
    		logToFile("building cursor"+key);
    		handleRequest(key, "remove", pendingQueries);
      		 outputMap=result;
       		 hasCopied=false;
       		 while(!hasCopied);
    		//logToFile("building cursor --- DONE----");
    	}
    	else
    		handleRequest(key, "remove", pendingQueries);
   		 
   		logToFile("returning after handling"+key);
    	
    }
    	
 
     
    private  void handleSearchResult(String key,HashMap<String, String>result)
    {
    	//logToFile("handleSearchResult"+key);
    	String[] cols={"key","value"};
		 outputCursor=new MatrixCursor(cols);
		 Iterator<String> it=result.keySet().iterator();
		 while(it.hasNext())
		 {
			 String[] value=new String[2];
			 value[0]=it.next();
			 value[1]=result.get(value[0]).trim();
			 outputCursor.addRow(value);
			 
		 }
		 handleRequest("*", "delete", pendingQueries);
		 //logToFile("returning cursor+ "+String.valueOf(outputCursor.getCount()));
    }
    public void handleRequest(String key,String type,Map<String,Integer>operationMap)
    {
    	
    	//logToFile("loggin request"+key+"----->"+type+operationMap.keySet().toString());
    	int value=operationMap.containsKey(key)?operationMap.get(key):0;
    	if(type.equals("add"))
    		value++;
    	else
    		value--;
    	
    	//if(value>0)
    		operationMap.put(key, value);
//    	else if(operationMap.containsKey(key))
//    		operationMap.remove(key);
    	
    	//logToFile("returnging from handlerequest"+key);
    	//logToFile(operationMap.entrySet().toString());
    
    }
    
    public synchronized void updateVersions(String key,VersionObject vOb)
    {
    	
    	if(!versions.containsKey(key) || vOb.versionNumber>versions.get(key).versionNumber)
    	{
    		//logToFile("updating versions for:"+key+" with "+String.valueOf(vOb.versionNumber));
    		versions.put(key, vOb);
    	}
    	
    	//logToFile("VERSIONS:"+versions.entrySet().toString());
    		
    }
      
    //MessageHandler
    
	public class MessageHandler implements Runnable
	{
		MessageBody incomingMessage;
		
		public MessageHandler(MessageBody inc)
		{
			incomingMessage=inc;
			
		}
		
		public void run()
		{
			 
			 if (incomingMessage.messageType.equals("InsertKey"))
			 { 
				// key value pair to Insert
				 
				String key=String.valueOf(incomingMessage.parameters.get(1));
				String value=String.valueOf(incomingMessage.parameters.get(2));
				String repCount=String.valueOf(incomingMessage.parameters.get(3));
				//logToFile("Recv: "+incomingMessage.parameters.toString());
				//code for insertion
				ContentValues  keyValueToInsert = new ContentValues();
				// inserting <”key-to-insert”, “value-to-insert”>
				keyValueToInsert.put("key", key);
				keyValueToInsert.put("value", value);
				keyValueToInsert.put("repCount", repCount);
				Uri newUri =  SimpleDynamoProvider.this.insert(CONTENT_URI,keyValueToInsert);
				int senderPort=2*Integer.parseInt(String.valueOf(incomingMessage.parameters.get(0)));
				ArrayList<String> param=new ArrayList<String>();
				param.add(key);
				MessageBody sentMessage=new MessageBody("InsertComplete",param,null,null);
				new ClientThread(sentMessage,String.valueOf(senderPort)).start();
				//logToFile(sentMessage);
				
			 }
			 else if (incomingMessage.messageType.equals("InsertComplete"))
			 {
				 //pendingInserts.remove(incomingMessage.parameters.get(0));
				 handleRequest(String.valueOf(incomingMessage.parameters.get(0)), "delete", pendingInserts);
				 //logToFile("done here , returning");
			 }
			 else if (incomingMessage.messageType.equals("SearchKey"))
			 	{// Query for a particular key has arrived
				 
				 String searchKey=String.valueOf(incomingMessage.parameters.get(0));
				 String sender=String.valueOf(incomingMessage.parameters.get(1));
				 
				 if(searchKey.equals("*")) // results accumulatred tme to send back
				 {
					Cursor resultCursor;
					if(sender.equals(String.valueOf(AVDNumber)))
					{
						//logToFile("total result obtained now sending back\n");
						MessageBody mssgSent=new MessageBody("SearchResult",incomingMessage.parameters,incomingMessage.keyValuePairs,null);
						//logToFile(mssgSent);
						new ClientThread(mssgSent,String.valueOf(2*AVDNumber)).start();
						
					}
					else
					{
						resultCursor=SimpleDynamoProvider.this.query(CONTENT_URI, null,"@", null, null);
						while(resultCursor.moveToNext())
							incomingMessage.keyValuePairs.put(resultCursor.getString(0), resultCursor.getString(1));
						
						ArrayList<String> param=new ArrayList<String>();
						param.add(searchKey);
						param.add(sender);
						
		        		int tempsucc=successor;
		        		if(!isNeighbourActive())
		        		{
		        			tempsucc=Integer.parseInt(successorForNode(String.valueOf(successor)));
		        		}
		        		int succPort=2*tempsucc;
		        		MessageBody sentMessage=new MessageBody("SearchKey",param,incomingMessage.keyValuePairs,null);
						//logToFile(sentMessage);
						new ClientThread(sentMessage,String.valueOf(succPort)).start();
					}
				}
				else // single file searched for 
				{
					//if(isKeyOnCurrentNode(searchKey)) // will always be true // keyHash.compareTo(predecessorHash)>0 && keyHash.compareTo(currentNodeHash)<=0)
					//{
						HashMap<String, VersionObject> result=SimpleDynamoProvider.this.query(searchKey);
						
						//while(resultCursor.moveToNext())
							//incomingMessage.keyValuePairs.put(resultCursor.getString(0), resultCursor.getString(1));
						
						int senderPort=2*Integer.parseInt(sender);
						ArrayList<String> param=new ArrayList<String>();
						param.add(searchKey);
						
						new ClientThread(new MessageBody("SearchResult",param,null,result),String.valueOf(senderPort)).start();
						
					//}
//					else // will never be triggered in direct routing
//					{
//						int succPort=2*successor;
//						ArrayList<String> param=new ArrayList<String>();
//						param.add(searchKey);
//						param.add(sender);
//						new ClientThread(new MessageBody("SearchKey",incomingMessage.parameters,incomingMessage.keyValuePairs),String.valueOf(succPort)).start();
//						
//					}
					
					
				}
				 
				
				 
			 }
			 else if (incomingMessage.messageType.equals("SearchResult"))
			 {
				 // value found for the sent key
				
				 String searchKey=String.valueOf(incomingMessage.parameters.get(0));
				 if(searchKey.equals("*"))
				 {
					 HashMap<String, String>result=new HashMap<String, String>(incomingMessage.keyValuePairs);
					 handleSearchResult(new String(),result);
					 
				 }
				 else
				 { 
					 updateVersions(searchKey,incomingMessage.keyVersionPairs.get(searchKey));
					 HashMap<String, VersionObject>result=new HashMap<String, VersionObject>(incomingMessage.keyVersionPairs);
					 handleSearchResultSingle(new String(searchKey),result);

				 }

			 }
			 else if(incomingMessage.messageType.equals("DeleteKey"))
			 {
				 // Query for a particular key has arrived
				 
				 
				 
				 String deleteKey=String.valueOf(incomingMessage.parameters.get(0));
				 String sender=String.valueOf(incomingMessage.parameters.get(1));
				 
				 if(deleteKey.equals("*"))
				 {
					
					if(sender.equals(String.valueOf(AVDNumber)))
					{
						//logToFile("total result obtained now sending back\n");
						MessageBody mssgSent=new MessageBody("DeleteResult",incomingMessage.parameters,null,null);
						//logToFile(mssgSent);
						new ClientThread(mssgSent,String.valueOf(2*AVDNumber)).start();
						
					}
					else
					{
						int res=SimpleDynamoProvider.this.delete(CONTENT_URI, "@", null);
						
						int oldResult=Integer.parseInt(String.valueOf(incomingMessage.parameters.get(2)));
						res=oldResult&res;
						
						
						int succPort=2*successor;
						ArrayList<String> param=new ArrayList<String>();
						param.add(deleteKey);
						param.add(sender);
						param.add(String.valueOf(res));
						
						MessageBody sentMessage=new MessageBody("DeleteKey",param,null,null);
						//logToFile(sentMessage);
						new ClientThread(sentMessage,String.valueOf(succPort)).start();
					}
				}
				else // single file searched for 
				{
					//if(isKeyOnCurrentNode(deleteKey))// keyHash.compareTo(predecessorHash)>0 && keyHash.compareTo(currentNodeHash)<=0)
					
						int res=SimpleDynamoProvider.this.delete(deleteKey);
						int senderPort=2*Integer.parseInt(sender);
						ArrayList<String> param=new ArrayList<String>();
						param.add(deleteKey);
						param.add(sender);
						param.add(String.valueOf(res));
						new ClientThread(new MessageBody("DeleteResult",param,null,null),String.valueOf(senderPort)).start();
						
//					
//					else // should never be called
//					{
//						int succPort=2*successor;
//						ArrayList<String> param=new ArrayList<String>();
//						param.add(deleteKey);
//						param.add(sender);
//						new ClientThread(new MessageBody("DeleteKey",incomingMessage.parameters,null,null),String.valueOf(succPort)).start();
//						
//					}
//					
					
				}
				 
			 }
			 else if(incomingMessage.messageType.equals("DeleteResult"))
			 {
				 
				 finalDeleteResult=Integer.parseInt(String.valueOf(incomingMessage.parameters.get(2)));

				 //logToFile("Value recevied");
				 //logToFile(outputCursor.toString());

				 //isStillDeleting=false;
				// pendingDeletes.remove(incomingMessage.parameters.get(0));
				 handleRequest(String.valueOf(incomingMessage.parameters.get(0)), "remove", pendingDeletes);
 
			 }
			 else if (incomingMessage.messageType.equals("RecoverSelf"))
			 {
				 HashMap<String, VersionObject> localResult=SimpleDynamoProvider.this.query("@v");
				 ArrayList<String> param=new ArrayList<String>();
				 param.add("Self");
				 int senderPort=Integer.parseInt(incomingMessage.parameters.get(0))*2;
				 new ClientThread(new MessageBody("RecoverResult",param,null,localResult),String.valueOf(senderPort)).start();
			 }
			 else if (incomingMessage.messageType.equals("RecoverReplica"))
			 { 
				 HashMap<String, VersionObject> localResult=SimpleDynamoProvider.this.query("@v");
				 ArrayList<String> param=new ArrayList<String>();
				 param.add("Replica");
				 int senderPort=Integer.parseInt(incomingMessage.parameters.get(0))*2;
				 new ClientThread(new MessageBody("RecoverResult",param,null,localResult),String.valueOf(senderPort)).start();
			 
				 
			 }
			 else if (incomingMessage.messageType.equals("RecoverResult"))
			 {
				 recoveryStatus++;
				 SimpleDynamoProvider.this.recover(incomingMessage.parameters.get(0),incomingMessage.keyVersionPairs);
			 }
			 else if (incomingMessage.messageType.equals("PollNeighbourRequest"))
			 {
				 int sender=Integer.parseInt(incomingMessage.sender);
				 int destPort=sender*2;
				 new ClientThread(new MessageBody("PollNeighbourReply",new ArrayList<String>(),null,null),String.valueOf(destPort)).start();
			 }
			 else if (incomingMessage.messageType.equals("PollNeighbourReply"))
			 {
				 isNeighbourActive=true;
			 }
			  
		}
		
			 
	}
    
    
    
    //Socket Commincation Intefaces
    
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            
            try
            {
            	while(true) {
             	Socket serverSock=serverSocket.accept();
            	ObjectInputStream oStream=new ObjectInputStream(serverSock.getInputStream());
            	MessageBody m=(MessageBody)oStream.readObject();
        		SimpleDynamoProvider.this.logToFile("Incoming message----->");
        		SimpleDynamoProvider.this.logToFile(m);
        		//Executors.newSingleThreadExecutor().execute(new MessageHandler(m));
        		new MessageHandler(m).run();
        		//Executors.newSingleThreadExecutor().execute(new MessageHandler(m));
            	
            	oStream.close();
            	//serverSock.close();
            	}
            	
            }
            catch(Exception e)
            {
            	//logToFile("In reading ***"+e.getClass()+e.getStackTrace().toString());
            	//Log.d(TAG, e.getStackTrace().toString());
            	//System.out.println(e);
            }
            return null;
        }
        
  
   }
    public class ClientThread extends Thread
    {
    	private MessageBody mssg;
    	private String port;
    	public ClientThread(MessageBody m,String port)
    	{
    		mssg=m;
    		this.port=port;
    	}
    	public void run()
    	{

            try {
            	mssg.sender=String.valueOf(SimpleDynamoProvider.this.AVDNumber);
            	int portNum=Integer.parseInt(port);
            	//SimpleDynamoProvider.this.logToFile("OUTGOING MESSAGE TO PORT : ******"+ String.valueOf(portNum));
            	//SimpleDynamoProvider.this.logToFile(mssg);
        		Log.d(TAG,"Trying with port:"+String.valueOf(port)+"with mssg"+mssg);
        		SocketAddress destAddr= new InetSocketAddress("10.0.2.2", portNum);
        			
        		Socket socket = new Socket();//InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),portNum);
        		socket.connect(destAddr);

                ObjectOutputStream output=new ObjectOutputStream(socket.getOutputStream());
                SimpleDynamoProvider.this.logToFile("outgoing message----->port:"+port);
                SimpleDynamoProvider.this.logToFile(mssg);
   
                //output.writeBytes(mssg);
               output.writeObject(mssg);
                output.close();
                socket.close();
            } catch (UnknownHostException e) {
            	SimpleDynamoProvider.this.logToFile("ClientTask UnknownHostException");
            	isNeighbourActive=false;
               // Log.d(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
            	SimpleDynamoProvider.this.logToFile("ClientTask socket IOException .. connecting to "+port);
            	isNeighbourActive=false;
            	//Log.d(TAG, "ClientTask socket IOException");
            }

            
        
    	}
    }
	
    public void getAVDDetails()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String rt=tel.getLine1Number();
        String portStr = rt.substring(tel.getLine1Number().length() - 4);
        myPortStr = String.valueOf((Integer.parseInt(portStr) * 2));
        //AVDNumber=Arrays.asList(ports).indexOf(myPortStr);
        AVDNumber=Integer.parseInt(myPortStr)/2;
    }
    public void logToFile(MessageBody m)
    {
//    	
//    	try
//    	{
//        	File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),"logfile");
//        	String value=m.sender+"------------->"+m.messageType+"------>"+(m.parameters!=null ? m.parameters.toString():"  ")+"------->"+((m.nodeSet!=null)?m.nodeSet.toString():"  ")+"------>"+((m.keyValuePairs!=null)?m.keyValuePairs:"  ")+((m.keyVersionPairs!=null)?m.keyVersionPairs.toString():"  ")+"\n";
//        	if(!f.exists())
//        		f.createNewFile();
//        	
//        	
//      		FileWriter fileWritter = new FileWriter(f.getAbsolutePath(),true);
//	        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
//	        bufferWritter.write(value);
//	        bufferWritter.close();
////        	FileOutputStream outputStream = new FileOutputStream(f);//openFileOutput(filename, Context.MODE_PRIVATE);
////            outputStream.write(value.getBytes());
////            outputStream.close();
//    	}
//    	catch(IOException e)
//    	{
//    		e.printStackTrace();
//    	}

    }
    
    public void logToFile(String m)
    {
    	try
    	{
        	File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),"logfile");
        	String value=m+"\n";
        	if(!f.exists())
        		f.createNewFile();
        	
        	
      		FileWriter fileWritter = new FileWriter(f.getAbsolutePath(),true);
	        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
	        bufferWritter.write(value);
	        bufferWritter.close();
//        	FileOutputStream outputStream = new FileOutputStream(f);//openFileOutput(filename, Context.MODE_PRIVATE);
//            outputStream.write(value.getBytes());
//            outputStream.close();
    	}
    	
    	catch(IOException e)
    	{
    		e.printStackTrace();
    	}

    }
    
    
    private boolean isKeyOnNode(String key,String nodeHash,String predNodeHash)
    {
    	try
    	{
    		String keyHashString=genHash(key);
    		
    		return (keyHashString.compareTo(nodeHash)<=0 && keyHashString.compareTo(predNodeHash)>0) || (nodeHash.equals(smallestInRing)&&(keyHashString.compareTo(nodeHash)<=0 || keyHashString.compareTo(predNodeHash)>0));
    	}
    	catch(NoSuchAlgorithmException e)
    	{
    		e.printStackTrace();
    	}
    	
    	return false;
    }
    public boolean isKeyOnCurrentNode(String key)
    {
    	
    	return isKeyOnNode(key, currentNodeHash,predecessorHash);
    }
    
    private String nodeForKey(String key)
    {
    	try 
    	{
    		String predHash=genHash(nodeSet[4]); //jugadd hard coded count
	    	for(String s:nodeSet)
	    	{
	    		String nodeHash=genHash(s);
	    		if(isKeyOnNode(key,nodeHash,predHash)==true)
	    			return s;
	    		predHash=nodeHash;
	    	}
    	}
    	catch(NoSuchAlgorithmException e)
    	{
    		e.printStackTrace();
    	}
    	return "";
    }
    
    private String predecessorForNode(String nodenumber)
    {
		 int index=0;
		 String pred;
		 for(;index<nodeCount;index++)
		 {
			 if(nodeSet[index].equals(nodenumber))
				 break;
		 }

		 if(index==0)
			 pred=nodeSet[nodeCount-1];
		 else
			 pred=nodeSet[index-1];
		 
		 return pred;
    }
    
    private String successorForNode(String nodenumber)
    {
		 int index=0;
		 String succ;
		 for(;index<nodeCount;index++)
		 {
			 if(nodeSet[index].equals(nodenumber))
				 break;
		 }

		 if(index==nodeCount-1)
			 succ=nodeSet[0];
		 else
			 succ=nodeSet[index+1];
		 
		 return succ;
    }
    
    
    private void setupRing()
    {
    	//setup the neighbour info
    	
    	//Assumption: Only one of the nodes will fail. When this function is called (at startup) no other node will have failed. 
    	
		 try
		 {
			 largestInRing= genHash(nodeSet[nodeCount-1]);
			 smallestInRing=genHash(nodeSet[0]);
			 String predStr=null,succStr;
			 int index=0;
			 for(;index<nodeCount;index++)
			 {
				 if(nodeSet[index].equals(String.valueOf(AVDNumber)))
					 break;
			 }

			 if(index==0)
				predecessor=Integer.parseInt(nodeSet[nodeCount-1]);
			 else
				 predecessor=Integer.parseInt(nodeSet[index-1]);
			 
			 
			 if(index==nodeCount-1)
				 successor=Integer.parseInt(nodeSet[0]);
			 else
				 successor=Integer.parseInt(nodeSet[index+1]);
				 
			 predecessorHash=genHash(String.valueOf(predecessor));
			 successorHash=genHash(String.valueOf(successor));
				
			 
//			 // Write code for node split
//			 if(isDataSetInitialized==false)
//			 {
//				 ArrayList<String> param=new ArrayList<String>();
//				 param.add(String.valueOf(AVDNumber));
//				 MessageBody splitReqMssg=new MessageBody("SplitRequest", param,new HashMap<String, String>());
//				 new ClientThread(splitReqMssg,String.valueOf(2*successor)).start();
//			 }
		 }
		 catch(NoSuchAlgorithmException ex)
		 {
			 ex.printStackTrace();
		 }
    	
    	
    }
    private boolean isRecoveryNeeded()
    {

//    	if(AVDNumber==5554)
//    	{
//
//        	File f=new File(this.getContext().getFilesDir().getAbsoluteFile(),"logfile");
//        	return f.exists();
//    	}
    	
    	return true;//isNeighbourActive();

    	
    }
    private void activateRecovery()
	{
    	logToFile("Recovery in progress");
    	String succ=successorForNode(String.valueOf(AVDNumber));
    	String pred=predecessorForNode(String.valueOf(AVDNumber));
    	String prepred=predecessorForNode(pred);
    	
    	ArrayList<String> param=new ArrayList<String>();
    	param.add(String.valueOf(AVDNumber));
    	
    	int destPort=Integer.parseInt(succ)*2;
    	MessageBody selfMessage=new MessageBody("RecoverSelf",param, null, null);
    	new ClientThread(selfMessage,String.valueOf(destPort)).start();
    	
    	destPort=Integer.parseInt(pred)*2;
    	MessageBody repMessage=new MessageBody("RecoverReplica",param, null, null);
    	new ClientThread(repMessage,String.valueOf(destPort)).start();
    	
//    	destPort=Integer.parseInt(prepred)*2;
//    	repMessage=new MessageBody("RecoverReplica",param, null, null);
//    	new ClientThread(repMessage,String.valueOf(destPort)).start();

    }
	private void recover(String type,HashMap<String,VersionObject>data)
	{
		logToFile("RECOVERING");
		if(type.equals("Self"))
		{
			String selfStr=String.valueOf(AVDNumber);
			for(String key:data.keySet())
			{
				
				if(nodeForKey(key).equals(selfStr))
				{
					VersionObject obj=data.get(key);
					insert(key,obj);
				}
			}
			
			recoveryStatus--;
		}
		else
		{
			String selfStr=String.valueOf(AVDNumber);
			String pred=predecessorForNode(selfStr);
			String prepred=predecessorForNode(pred);
			for(String key:data.keySet())
			{
				VersionObject obj=data.get(key);
				if(nodeForKey(key).equals(pred) || nodeForKey(key).equals(prepred))
				{
					insert(key,obj);
				}
			}
			
			recoveryStatus--;
			
			
		}
			
			

	}
	public boolean isNeighbourActive()
	{
		isNeighbourActive=false;
		new ClientThread(new MessageBody("PollNeighbourRequest",new ArrayList<String>(),null,null),String.valueOf(successor*2)).start();
		try
		{
			Thread.sleep(2);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		if(isNeighbourActive)
			logToFile(String.valueOf(successor)+" is active");
		else
			logToFile(String.valueOf(successor)+" is inactive");
		return isNeighbourActive;
	}

}


	