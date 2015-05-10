package edu.wisc.cs.sdn.apps.loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFOXMFieldType;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.action.OFActionSetField;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;
import org.openflow.protocol.instruction.OFInstructionGotoTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.cs.sdn.apps.l3routing.L3Routing;
import edu.wisc.cs.sdn.apps.util.ArpServer;
import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.internal.DeviceManagerImpl;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.util.MACAddress;

public class LoadBalancer implements IFloodlightModule, IOFSwitchListener,
		IOFMessageListener
{
	public static final String MODULE_NAME = LoadBalancer.class.getSimpleName();
	
	private static final byte TCP_FLAG_SYN = 0x02;
	
	private static final short IDLE_TIMEOUT = 20;
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;
    
    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    private byte table;
    
    // Set of virtual IPs and the load balancer instances they correspond with
    private Map<Integer,LoadBalancerInstance> instances;

    /**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		
		// Obtain table number from config
		Map<String,String> config = context.getConfigParams(this);
        this.table = Byte.parseByte(config.get("table"));
        
        // Create instances from config
        this.instances = new HashMap<Integer,LoadBalancerInstance>();
        String[] instanceConfigs = config.get("instances").split(";");
        for (String instanceConfig : instanceConfigs)
        {
        	String[] configItems = instanceConfig.split(" ");
        	if (configItems.length != 3)
        	{ 
        		log.error("Ignoring bad instance config: " + instanceConfig);
        		continue;
        	}
        	LoadBalancerInstance instance = new LoadBalancerInstance(
        			configItems[0], configItems[1], configItems[2].split(","));
            this.instances.put(instance.getVirtualIP(), instance);
            log.info("Added load balancer instance: " + instance);
        }
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        /*********************************************************************/
        /* TODO: Initialize other class variables, if necessary              */
        /*********************************************************************/
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFSwitchListener(this);
		this.floodlightProv.addOFMessageListener(OFType.PACKET_IN, this);
		
		/*********************************************************************/
		/* TODO: Perform other tasks, if necessary                           */
		/*********************************************************************/
		
	}
	
	/**
     * Event handler called when a switch joins the network.
     * @param DPID for the switch
     */
	@Override
	public void switchAdded(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d added", switchId));
		
		/*********************************************************************/
		/* TODO: Install rules to send:                                      */
		/*       (1) packets from new connections to each virtual load       */
		/*       balancer IP to the controller                               */
		/*       (2) ARP packets to the controller, and                      */
		/*       (3) all other packets to the next rule table in the switch  */
		/*********************************************************************/
		List<OFInstruction> instructions = null;

		//INSTALL RULE FOR: all entries for each virtual IP to each switch that is added to the network
		for(Integer virtIP : instances.keySet()){
			OFInstructionApplyActions instruction = null;
			
			OFMatch matchCriteria = new OFMatch();
			matchCriteria.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
			matchCriteria.setNetworkDestination(virtIP);

			//creating actions for what to do
			List<OFAction> actions = new ArrayList<OFAction>();
			OFActionOutput action;
			
			//send matches to controller
			action = new OFActionOutput(OFPort.OFPP_CONTROLLER);
			actions.add(action);
			instructions = new ArrayList<OFInstruction>();
			instruction = new OFInstructionApplyActions(actions);
			instructions.add(instruction);
			
			SwitchCommands.installRule(sw, table,(short) (SwitchCommands.DEFAULT_PRIORITY + 1), matchCriteria, instructions);
		}
		
		//INSTALL RULE FOR: blank match criteria for all other packets
		OFMatch empty = new OFMatch();
		
		//if not for one of the virtual IP's, use the L3 Routing table to determine what to do.
		instructions = new ArrayList<OFInstruction>();
		OFInstructionGotoTable instruction = new OFInstructionGotoTable(L3Routing.table);
		instructions.add(instruction);
		
		SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, empty, instructions);
	}
	
	/**
	 * Handle incoming packets sent from switches.
	 * @param sw switch on which the packet was received
	 * @param msg message from the switch
	 * @param cntx the Floodlight context in which the message should be handled
	 * @return indication whether another module should also process the packet
	 */
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) 
	{
		// We're only interested in packet-in messages
		if (msg.getType() != OFType.PACKET_IN)
		{ return Command.CONTINUE; }
		OFPacketIn pktIn = (OFPacketIn)msg;
		
		// Handle the packet
		Ethernet ethPkt = new Ethernet();
		ethPkt.deserialize(pktIn.getPacketData(), 0,
				pktIn.getPacketData().length);
		
		/*********************************************************************/
		/* TODO: Send an ARP reply for ARP requests for virtual IPs; for TCP */
		/*       SYNs sent to a virtual IP, select a host and install        */
		/*       connection-specific rules to rewrite IP and MAC addresses;  */
		/*       ignore all other packets                                    */
		
		/*********************************************************************/
		
		//Verify that it is IPv4
		if(ethPkt.getEtherType() == Ethernet.TYPE_IPv4){
			IPv4 iPkt = (IPv4) ethPkt.getPayload();
			if(iPkt.getProtocol() == IPv4.PROTOCOL_TCP){
				TCP tPkt = (TCP) iPkt.getPayload();
				if(tPkt.getFlags() == TCP_FLAG_SYN){
					//We have a valid TCP SYN
					handleTCPSyn(iPkt, sw);
				}
			}
		}
		if(ethPkt.getEtherType() == Ethernet.TYPE_ARP){
			handleArpPkt(ethPkt, sw, (short)pktIn.getInPort());
		}
		// We don't care about other packets
		return Command.CONTINUE;
	}
	
	private void handleTCPSyn(IPv4 iPkt, IOFSwitch sw){
		TCP tPkt = (TCP) iPkt.getPayload();
		List<OFInstruction> instructions = new ArrayList<OFInstruction>();
		List<OFAction> actions = new ArrayList<OFAction>();
		
		/////RULES FOR COMING IN//////
		int dstVirtualIp = iPkt.getDestinationAddress();
		int nextAvailableConnection = this.instances.get(dstVirtualIp).getNextHostIP();
		
		//Set Match criteria
		OFMatch matchCriteria = new OFMatch();
		//ip
		matchCriteria.setDataLayerType(Ethernet.TYPE_IPv4);
		matchCriteria.setNetworkSource(iPkt.getSourceAddress());
		matchCriteria.setNetworkDestination(dstVirtualIp);
		//tcp
		matchCriteria.setNetworkProtocol(OFMatch.IP_PROTO_TCP);
		matchCriteria.setTransportSource(OFMatch.IP_PROTO_TCP, tPkt.getSourcePort());
		matchCriteria.setTransportDestination(OFMatch.IP_PROTO_TCP, tPkt.getDestinationPort());
		
		//create instructions for mac and ip translation
		//as well as a "default case" instruction to use the L3routing table 
		OFActionSetField ip = new OFActionSetField(OFOXMFieldType.IPV4_DST, nextAvailableConnection);
		actions.add(ip);
		OFActionSetField mac = new OFActionSetField(OFOXMFieldType.ETH_DST, this.getHostMACAddress(nextAvailableConnection));
		actions.add(mac);
		
		OFInstructionApplyActions instruction = new OFInstructionApplyActions(actions);
		instructions.add(instruction);
		OFInstructionGotoTable defaultCase = new OFInstructionGotoTable(L3Routing.table);
		instructions.add(defaultCase);
		
		SwitchCommands.installRule(sw, table, (short)(SwitchCommands.DEFAULT_PRIORITY + 2), matchCriteria,
				instructions, SwitchCommands.NO_TIMEOUT, IDLE_TIMEOUT);
	
		
		/////RULES FOR GOING OUT//////
		instructions = new ArrayList<OFInstruction>();
		actions = new ArrayList<OFAction>();
		matchCriteria = new OFMatch();

		//ip
		matchCriteria.setDataLayerType(Ethernet.TYPE_IPv4);
		matchCriteria.setNetworkSource(nextAvailableConnection);
		matchCriteria.setNetworkDestination(iPkt.getSourceAddress());
		//tcp
		matchCriteria.setNetworkProtocol(OFMatch.IP_PROTO_TCP);
		matchCriteria.setTransportSource(OFMatch.IP_PROTO_TCP, tPkt.getDestinationPort());
		matchCriteria.setTransportDestination(OFMatch.IP_PROTO_TCP, tPkt.getSourcePort());
		
		//Instructions: Modify IP and MAC
		ip = new OFActionSetField(OFOXMFieldType.IPV4_SRC, dstVirtualIp);
		actions.add(ip);
		mac = new OFActionSetField(OFOXMFieldType.ETH_SRC, this.instances.get(dstVirtualIp).getVirtualMAC());
		actions.add(mac);
		
		instruction = new OFInstructionApplyActions(actions);
		instructions.add(instruction);
		defaultCase = new OFInstructionGotoTable(L3Routing.table);
		instructions.add(defaultCase);
		
		SwitchCommands.installRule(sw, table, (short)(SwitchCommands.DEFAULT_PRIORITY + 2), matchCriteria,
				instructions, SwitchCommands.NO_TIMEOUT, IDLE_TIMEOUT);
	}
	
	private void handleArpPkt(Ethernet ethPkt, IOFSwitch sw, short outport){
		ARP arpPkt = (ARP) ethPkt.getPayload();
		int virtIP = IPv4.toIPv4Address(arpPkt.getTargetProtocolAddress());
		
		//If we have an Arp request destined for one of our virtual devices
		if(this.instances.containsKey(virtIP)){
			byte[] mac = this.instances.get(virtIP).getVirtualMAC();
			//we will construct a reply packet
			ARP reply = new ARP();
			Ethernet eth = new Ethernet();

			
			//Set up the ethernet header
			eth.setEtherType(Ethernet.TYPE_ARP);
			eth.setDestinationMACAddress(ethPkt.getSourceMACAddress());
			eth.setSourceMACAddress(mac);
			
			//setup the arp header
			//might not need all these fields, but lets set them all anyways
			reply.setOpCode(ARP.OP_REPLY);
			//arp protocol
			reply.setProtocolAddressLength(arpPkt.getProtocolAddressLength());
			reply.setSenderProtocolAddress(virtIP);
			reply.setProtocolType(arpPkt.getProtocolType());
			reply.setTargetProtocolAddress(arpPkt.getSenderProtocolAddress());
			//arp Hardware
			reply.setHardwareAddressLength(arpPkt.getHardwareAddressLength());
			reply.setSenderHardwareAddress(mac);
			reply.setHardwareType(arpPkt.getHardwareType());
			reply.setTargetHardwareAddress(arpPkt.getSenderHardwareAddress());
			
			//link the headers together
			eth.setPayload(reply);
			
			SwitchCommands.sendPacket(sw, outport, eth);	
		}
	}
	
	/**
	 * Returns the MAC address for a host, given the host's IP address.
	 * @param hostIPAddress the host's IP address
	 * @return the hosts's MAC address, null if unknown
	 */
	private byte[] getHostMACAddress(int hostIPAddress)
	{
		Iterator<? extends IDevice> iterator = this.deviceProv.queryDevices(
				null, null, hostIPAddress, null, null);
		if (!iterator.hasNext())
		{ return null; }
		IDevice device = iterator.next();
		return MACAddress.valueOf(device.getMACAddress()).toBytes();
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{ /* Nothing we need to do, since the switch is no longer active */ }

	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId)
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when a port on a switch goes up or down, or is
	 * added or removed.
	 * @param DPID for the switch
	 * @param port the port on the switch whose status changed
	 * @param type the type of status change (up, down, add, remove)
	 */
	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) 
	{ /* Nothing we need to do, since load balancer rules are port-agnostic */}

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }
	
    /**
     * Tell the module system which services we provide.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() 
	{ return null; }

	/**
     * Tell the module system which services we implement.
     */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> 
			getServiceImpls() 
	{ return null; }

	/**
     * Tell the module system which modules we depend on.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> 
			getModuleDependencies() 
	{
		Collection<Class<? extends IFloodlightService >> floodlightService =
	            new ArrayList<Class<? extends IFloodlightService>>();
        floodlightService.add(IFloodlightProviderService.class);
        floodlightService.add(IDeviceService.class);
        return floodlightService;
	}

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) 
	{
		return (OFType.PACKET_IN == type 
				&& (name.equals(ArpServer.MODULE_NAME) 
					|| name.equals(DeviceManagerImpl.MODULE_NAME))); 
	}

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) 
	{ return false; }
}
