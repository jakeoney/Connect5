package edu.wisc.cs.sdn.apps.l3routing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.cs.sdn.apps.util.Host;
import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;

public class L3Routing implements IFloodlightModule, IOFSwitchListener, 
ILinkDiscoveryListener, IDeviceListener
{
	public static final String MODULE_NAME = L3Routing.class.getSimpleName();

	// Interface to the logging system
	private static Logger log = LoggerFactory.getLogger(MODULE_NAME);

	// Interface to Floodlight core for interacting with connected switches
	private IFloodlightProviderService floodlightProv;

	// Interface to link discovery service
	private ILinkDiscoveryService linkDiscProv;

	// Interface to device manager service
	private IDeviceService deviceProv;

	// Switch table in which rules should be installed
	public static byte table;

	// Map of hosts to devices
	private Map<IDevice,Host> knownHosts;

	/**
	 * Loads dependencies and initializes data structures.
	 */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		Map<String,String> config = context.getConfigParams(this);
		table = Byte.parseByte(config.get("table"));

		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
		this.linkDiscProv = context.getServiceImpl(ILinkDiscoveryService.class);
		this.deviceProv = context.getServiceImpl(IDeviceService.class);

		this.knownHosts = new ConcurrentHashMap<IDevice,Host>();
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
		this.linkDiscProv.addListener(this);
		this.deviceProv.addListener(this);
		/*********************************************************************/
		/* TODO: Initialize variables or perform startup tasks, if necessary */		
		/*********************************************************************/
	}

	/**
	 * Get a list of all known hosts in the network.
	 */
	private Collection<Host> getHosts()
	{ return this.knownHosts.values(); }

	/**
	 * Get a map of all active switches in the network. Switch DPID is used as
	 * the key.
	 */
	private Map<Long, IOFSwitch> getSwitches()
	{ return floodlightProv.getAllSwitchMap(); }

	/**
	 * Get a list of all active links in the network.
	 */
	private Collection<Link> getLinks()
	{ return linkDiscProv.getLinks().keySet(); }

	/**
	 * Event handler called when a host joins the network.
	 * @param device information about the host
	 */
	@Override
	public void deviceAdded(IDevice device) 
	{
		Host host = new Host(device, this.floodlightProv);
		// We only care about a new host if we know its IP
		if (host.getIPv4Address() != null)
		{
			log.info(String.format("Host %s added", host.getName()));

			/*****************************************************************/
			/* TODO: Update routing: add rules to route to new host          */
			/*****************************************************************/

			this.knownHosts.put(device, host);
			bellmanFord(host);
		}
	}

	/**
	 * Event handler called when a host is no longer attached to a switch.
	 * @param device information about the host
	 */
	@Override
	public void deviceRemoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{ return; }
		this.knownHosts.remove(host);

		log.info(String.format("Host %s is no longer attached to a switch", 
				host.getName()));

		/*********************************************************************/
		/* TODO: Update routing: remove rules to route to host               */
		/*********************************************************************/
		//if a host is removed, just iterate through each switch and remove that entry
		for(Entry<Long, IOFSwitch> currSw : this.getSwitches().entrySet()){
			OFMatch matchCriteria = new OFMatch();
			matchCriteria.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
			matchCriteria.setNetworkDestination(host.getIPv4Address());
			SwitchCommands.removeRules(currSw.getValue(), table, matchCriteria);
		}
	}

	/**
	 * Event handler called when a host moves within the network.
	 * @param device information about the host
	 */
	@Override
	public void deviceMoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}

		if (!host.isAttachedToSwitch())
		{
			this.deviceRemoved(device);
			return;
		}
		log.info(String.format("Host %s moved to s%d:%d", host.getName(),
				host.getSwitch().getId(), host.getPort()));

		/*********************************************************************/
		/* TODO: Update routing: change rules to route to host               */
		/*********************************************************************/
		log.info(String.format("Host %s moved to s%d:%d", host.getName(),
				host.getSwitch().getId(), host.getPort()));
		
		//adding a new entry for the given host should re-create the current entries for that host
		bellmanFord(host);
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
		/* TODO: Update routing: change routing rules for all hosts          */
		/*********************************************************************/
		
		//if network state changes, just recalculate bellmanFord
		for(Host host : this.getHosts()){
			bellmanFord(host);
		}
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d removed", switchId));

		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */		
		/*********************************************************************/
		for(Host host : this.getHosts()){
			if(host.isAttachedToSwitch()){
				bellmanFord(host);
			}
			else{
				for(Entry<Long, IOFSwitch> currSw : this.getSwitches().entrySet()){
					OFMatch matchCriteria = new OFMatch();
					matchCriteria.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
					matchCriteria.setNetworkDestination(host.getIPv4Address());
					SwitchCommands.removeRules(currSw.getValue(), table, matchCriteria);
				}
			}
		}
	}

	/**
	 * Event handler called when multiple links go up or down.
	 * @param updateList information about the change in each link's state
	 */
	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) 
	{
		for (LDUpdate update : updateList)
		{
			// If we only know the switch & port for one end of the link, then
			// the link must be from a switch to a host
			if (0 == update.getDst())
			{
				log.info(String.format("Link s%s:%d -> host updated", 
						update.getSrc(), update.getSrcPort()));
			}
			// Otherwise, the link is between two switches
			else
			{
				log.info(String.format("Link s%s:%d -> s%s:%d updated", 
						update.getSrc(), update.getSrcPort(),
						update.getDst(), update.getDstPort()));
			}
		}

		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		/*********************************************************************/
		//recompute the shortest path
		for(Host host : this.getHosts()){
			bellmanFord(host);
		}
	}

	/**
	 * Event handler called when link goes up or down.
	 * @param update information about the change in link state
	 */
	@Override
	public void linkDiscoveryUpdate(LDUpdate update) 
	{ this.linkDiscoveryUpdate(Arrays.asList(update)); }

	/**
	 * Event handler called when the IP address of a host changes.
	 * @param device information about the host
	 */
	@Override
	public void deviceIPV4AddrChanged(IDevice device) 
	{ this.deviceAdded(device); }

	/**
	 * Event handler called when the VLAN of a host changes.
	 * @param device information about the host
	 */
	@Override
	public void deviceVlanChanged(IDevice device) 
	{ /* Nothing we need to do, since we're not using VLANs */ }

	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId) 
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }

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
	{ /* Nothing we need to do, since we'll get a linkDiscoveryUpdate event */ }

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return this.MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) 
	{ return false; }

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) 
	{ return false; }

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
		floodlightService.add(ILinkDiscoveryService.class);
		floodlightService.add(IDeviceService.class);
		return floodlightService;
	}

	private void bellmanFord(Host srcHost) {
		//not sure if this needs to be synchronized
		synchronized(this){
		Vertex tmp;
		List<Vertex> switches = new ArrayList<Vertex>();
		IOFSwitch src = srcHost.getSwitch();
		for (IOFSwitch sw : this.getSwitches().values()) {
			if (sw.equals(src))
				tmp = new Vertex(sw, 0);
			else
				tmp = new Vertex(sw, Integer.MAX_VALUE - 1);
			switches.add(tmp);
		}
		//find neighbors
		newConnectionAdded(switches);

		// Finding shortest paths
		for (int i = 1; i < switches.size() - 1; i++) {
			for (Vertex curr: switches) {
				for (int port: curr.getSwitch().getEnabledPortNumbers()) {
					Vertex neighbor = curr.getConected().get(port);
					if(neighbor != null){
						if (curr.getDistance() > neighbor.getDistance() + 1) {
							curr.setDistance(neighbor.getDistance() + 1);
							curr.setOutPort(port);
						}
					}
				}
			}
		}

		//Install a rule for each switch
		for (Vertex dstSw: switches) {
			if (!dstSw.getSwitch().equals(src)) {
				installRule(srcHost, dstSw.getSwitch(), dstSw.getOutPort());
			}
			else{
				//for srcSw
				installRule(srcHost, src, srcHost.getPort());
			}
		}
		}
	}

	private boolean installRule(Host host, IOFSwitch sw, int port){
		List<OFInstruction> instructions = null;
		OFInstructionApplyActions instruction = null;
		int bufferId = 0;

		//if we are connected
		if(host.isAttachedToSwitch()){
			//setting match criteria
			OFMatch matchCriteria = new OFMatch();
			matchCriteria.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
			matchCriteria.setNetworkDestination(host.getIPv4Address());

			//creating actions for what to do
			List<OFAction> actions = new ArrayList<OFAction>();
			OFActionOutput action;

			action = new OFActionOutput(port);
			actions.add(action);
			instructions = new ArrayList<OFInstruction>();
			instruction = new OFInstructionApplyActions(actions);
			instructions.add(instruction);
			bufferId = action.getMaxLength();
			SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, matchCriteria, instructions,
					SwitchCommands.NO_TIMEOUT, SwitchCommands.NO_TIMEOUT, bufferId);
			return true;

		}
		return false;
	}

	//kinda messy, but this is storing a map of all "switch neighbors" and what port they are connected to.
	//along with what node is the outport
	private void newConnectionAdded(List<Vertex> switches) {
		for (Vertex currNode: switches) {
			//for each enabled port on currNode
			for (int currNodesPort: currNode.getSwitch().getEnabledPortNumbers()) {
				//look at all possible links for links connected to currNode's port
				for (Link link: this.getLinks()) {
					if ((link.getSrcPort() == currNodesPort) && (link.getSrc() == currNode.getSwitch().getId())) {
						//check all other potential nodes to see if they are connected
						for (Vertex potentialNeighbor: switches) {
							if(!(potentialNeighbor.getSwitch().equals(currNode.getSwitch()))){
								if ((potentialNeighbor.getSwitch().getEnabledPortNumbers().contains((int)link.getDstPort()))
										&& link.getDst() == potentialNeighbor.getSwitch().getId()) {
									currNode.setOutPort(currNodesPort);
									currNode.connected(currNodesPort, potentialNeighbor);
									break; //only one connection per link
								}
							}
						}
						//think i can break here too
						//break;
					}
				}
			}
		}	
	}
}