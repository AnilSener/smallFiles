package com.imshealthcare

import nifi.client.*
import java.io.*;
import java.util.*;

/**
 * Created by Anil Sener on 02/08/16.
 * @author: ANIL SENER
 */

class NiFi_Data_Collection_Client {
	private static final Properties prop=loadProperties();
	private static final String URL = 'http://'+prop.getProperty("NIFI_HOST")+':'+prop.getProperty("NIFI_PORT");

    static void main(String[] args) {

    /*FileInputStream fis = null;
    HashMap<String,ArrayList<String[]>> hm =new HashMap<String,ArrayList<String[]>>();
	try{
		fis = new FileInputStream("source_list.txt");
		int cnt = 0;
		Scanner s = new Scanner(fis);
		String k= null;
        while (s.hasNextLine()) {
            if((cnt+1)%3==1){
                k=s.nextLine()
                hm.put(k,new ArrayList<String[]>())
            }
            else{
                hm.get(k).add(s.nextLine())
            }
        cnt++;
        }
	}
	catch (IOException e)
	{
	    e.printStackTrace();
	}
	finally
	{
		if (fis != null) fis.close();
	}*/
	try{
		NiFi nifi = NiFi.bind(URL);
		println '\nProcessors\n-------------------'
		nifi.processors.each { k, v -> println "$k (${v.id}) is ${v.state}" }
		println '\nOriginal Templates\n-------------------'
		nifi.templates.each { k, v -> println "$k (${v.id})" }

		String template_name=prop.getProperty("TEMPLATE").replaceAll("\"","");
        Template t=nifi.templates.get(template_name)

		if(t==null){

			//def template = new XmlSlurper().parseText(this.getClass().getResource( "/"+template_name+".xml" ).text)
			//def template = this.getClass().getResource( "/"+template_name+".xml" )
			InputStream inputStream = null;
			FileOutputStream  outputStream = null;

			try {
				inputStream = this.getClass().getResourceAsStream("/" + template_name + ".xml")

				File template = File.createTempFile(template_name, ".xml");
				outputStream = new FileOutputStream(template)
				int read = 0;
				byte[] bytes = new byte[1024];

				while ((read = inputStream.read(bytes)) != -1) {
					outputStream.write(bytes, 0, read);
				}
				//System.out.println(template.text())
				nifi.templates.importTemplate(template)
				System.out.println("New Template: " + template_name + ".xml is imported to NIFI.")
				t = nifi.templates.get(template_name)
			}
			catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (IOException e) {
						System.out.println("NIFI Template "+ template_name +" cannot be imported to NIFI.")
						e.printStackTrace();
						System.exit(-1);
					}
				}
			}
        }
		nifi.templates.get(template_name).instantiate()
		nifi.processors.each{p -> p.getValue().start()}
		int activeThreadCount=0;
		String queued="";
		while(true){
		//Wait
            try{
                int period=Integer.parseInt(prop.getProperty("FLOW_VALID_PER_IN_SECONDS"))
                if(period>=0){
					System.out.println("System will wait "+period+" seconds to start validation process.");
					Thread.sleep(period*1000);
					activeThreadCount=nifi.controller.getActiveThreadCount()
					queued=nifi.controller.getQueued()
					System.out.println("Active Thread Count: "+activeThreadCount)
					System.out.println("Queued Data: "+queued)
				}
                else{
                    System.out.println("FLOW_VALID_PER_IN_SECONDS parameter that you have specified isn't a natural number. Please define an natural number in seconds.");
                    System.exit(-1);
                }
            }
            catch(NumberFormatException ne){
                System.out.println("FLOW_VALID_PER_IN_SECONDS parameter that you have specified isn't a natural number. Please define an natural number in seconds.");
                ne.printStackTrace();
                System.exit(-1);
            }
            System.out.println("Validating Data Collection...");
			""
            boolean valRes=(activeThreadCount==0 && queued.startsWith("0"));
			if(activeThreadCount==0 && !queued.startsWith("0")) System.exit(-2);
            System.out.println("Validation Result:"+valRes);
            if(valRes) break;
		}
		nifi.processors.each{p -> p.getValue().stop()}
        nifi.templates.get(template_name).delete()
		//nifi.processors.clear()
	}
	catch(Exception e){
		e.printStackTrace();
		System.exit(-1);
	}
    }
	/*static boolean validate(HashMap<String,ArrayList<String[]>> hm) {
		Runtime rt = Runtime.getRuntime();
		for (Map.Entry<Integer, String> entry : hm.entrySet()) {
			String dir=prop.getProperty("TARGET_DIR")+entry.getKey().substring(prop.getProperty("SOURCE_DIR").length(),entry.getKey().lastIndexOf("/"));
			System.out.println("Test:"+dir + " - " + entry.getValue());
			for (String file :entry.getValue()){
				System.out.println("hdfs dfs -find "+dir+" -name "+file);
				Process pr = rt.exec("hdfs dfs -find "+dir+" -name "+file);
				int retVal = pr.waitFor()
				//System.out.println(retVal)
				if(retVal!=0){
					return false;
				}
				else{
					BufferedReader reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				    	String line =reader.readLine();		
				    if (line != null) {
					System.out.println(line+" is found in HDFS.");
				    }
					else{return false;}
				}
			}
		}
		return true
	}*/
	static Properties loadProperties(){
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("application.properties");
			// load a properties file
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
			
		} finally {
			if (input != null) {
				try {
					input.close();
					return prop;
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
		}

	 }
}
