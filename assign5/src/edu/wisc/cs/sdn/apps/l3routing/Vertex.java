package edu.wisc.cs.sdn.apps.l3routing;

import java.util.HashMap;
import java.util.Map;

import net.floodlightcontroller.core.IOFSwitch;

public class Vertex {
	private  Map<Integer, Vertex> connected;
	private IOFSwitch sw;
	private int distance;
	private int outPort;
	
	/*public Vertex(){
		this.sw = null;
		this.distance = -1;
		this.connected = new HashMap<Integer, Vertex>();
	}*/
	
	public Vertex(IOFSwitch sw, int d) {
		this.sw = sw;
		this.distance = d;
		this.connected = new HashMap<Integer, Vertex>();
	}
	
	//Neighbors
	public Map<Integer, Vertex> getConected(){
		return this.connected;
	}
	
	public void connected(int port, Vertex sw) {
		connected.put(port, sw);
	}
	
	//DISTANCE
	public int getDistance() {
		return this.distance;
	}
	
	public void setDistance(int d) {
		this.distance = d;
	}
	
	//SWITCH
	public IOFSwitch getSwitch() {
		return this.sw;
	}
	
	public void setSwitch(IOFSwitch sw) {
		this.sw = sw;
	}

	//OUT PORT
	public int getOutPort() {
		return this.outPort;
	}
	
	public void setOutPort(int outPort) {
		this.outPort = outPort;
	}	
}
