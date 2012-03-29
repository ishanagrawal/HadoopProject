package org.myorg;
 
import java.io.IOException;
import java.util.*;
import java.lang.*;

public class NodeParser {

String pointType;
String line;
int pointId, objClassId, xCurrent, yCurrent, xNext, yNext;
float timestamp, speed;

	public String getPointType() {
		
		return pointType;
	}

	public int getPointId() {
		
		return pointId;
	}

	public int getObjClassId() {
		
		return objClassId;
	}

	public float getTimestamp() {
		
		return timestamp;
	}

	public int getXNext() {
		
		return xNext;
	}

	public int getYNext() {
		
		return yNext;
	}

	public int getXCurrent() {
		
		return xCurrent;
	}

	public int getYCurrent() {
		
		return yCurrent;
	}

	public float getSpeed() {
		
		return speed;
	}

	public void parse(String line) {

      	StringTokenizer tokenizer = new StringTokenizer(line);
      	while (tokenizer.hasMoreTokens()) {
		
		pointType = tokenizer.nextToken();	//point type

        	pointId = Integer.parseInt(tokenizer.nextToken());	//point id
		
		objClassId = Integer.parseInt(tokenizer.nextToken());  //object class id

		timestamp = Float.parseFloat(tokenizer.nextToken());  //timestamp

		xCurrent = Integer.parseInt(tokenizer.nextToken());  //x-coordinate

		yCurrent = Integer.parseInt(tokenizer.nextToken());  //y-coordinate 

		speed = Float.parseFloat(tokenizer.nextToken());  //current speed

		xNext = Integer.parseInt(tokenizer.nextToken());  //x-coordinate of the next node

		yNext = Integer.parseInt(tokenizer.nextToken());  //y-coordinate of the next node


      		}
	}
	


}
