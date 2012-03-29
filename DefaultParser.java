package org.myorg;
 
import java.io.IOException;
import java.util.*;
import java.lang.*;


public class DefaultParser {

String pointType;
int pointId, objClassId, timestamp, xNext, yNext;
float xCurrent, yCurrent, speed;


	public String getPointType() {
		
		return pointType;
	}

	public int getPointId() {
		
		return pointId;
	}

	public int getObjClassId() {
		
		return objClassId;
	}

	public int getTimestamp() {
		
		return timestamp;
	}

	public int getXNext() {
		
		return xNext;
	}

	public int getYNext() {
		
		return yNext;
	}

	public float getXCurrent() {
		
		return xCurrent;
	}

	public float getYCurrent() {
		
		return yCurrent;
	}

	public float getSpeed() {
		
		return speed;
	}

	public void parse(String line) {

      	StringTokenizer tokenizer = new StringTokenizer(line);
      	while (tokenizer.hasMoreTokens()) {
		
		//point type
		/*if("newpoint".equals(tokenizer.nextToken()))
			pointType = PointType.newpoint;
		else if("point".equals(tokenizer.nextToken()))
			pointType = PointType.point;
		else if("disappearpoint".equals(tokenizer.nextToken()))
			pointType = PointType.disappearpoint;		*/
		pointType = tokenizer.nextToken();

        	pointId = Integer.parseInt(tokenizer.nextToken());	//point id
		
		// Not required to store for now
		tokenizer.nextToken();	//sequence number
		
		objClassId = Integer.parseInt(tokenizer.nextToken());  //object class id

		timestamp = Integer.parseInt(tokenizer.nextToken());  //timestamp

		xCurrent = Float.parseFloat(tokenizer.nextToken());  //x-coordinate

		yCurrent = Float.parseFloat(tokenizer.nextToken());  //y-coordinate 

		speed = Float.parseFloat(tokenizer.nextToken());  //current speed

		xNext = Integer.parseInt(tokenizer.nextToken());  //x-coordinate of the next node

		yNext = Integer.parseInt(tokenizer.nextToken());  //y-coordinate of the next node


      		}
	}
	


}
