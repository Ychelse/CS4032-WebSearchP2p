package P2P;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Node extends Thread implements PeerSearchSimplified {
	private Map<String, String> routingTable; // <node_id, ipaddress>
	private Map<String, HashMap<String, Integer>> indexing; // <word, links>
	private int port;
	private DatagramSocket socket = null;
	private long node_id; // 2^7 for testing simplicity.

	public int getPort() {
		return this.port;
	}

	public Node() {
		super();
	}

	@Override
	public DatagramSocket init(DatagramSocket udp_socket) throws SocketException, UnknownHostException {
		
		this.port = udp_socket.getLocalPort();
		udp_socket.close();
		routingTable = new HashMap<String, String>();
		this.socket = new DatagramSocket(this.port);
		return udp_socket;
	}

	@Override
	public void init(int udp_socket) throws SocketException, UnknownHostException, InstantiationException, IllegalAccessException {
		this.port = udp_socket;
		routingTable = new HashMap<String, String>();
		indexing = new HashMap<String, HashMap<String, Integer>>();
		this.socket = new DatagramSocket(this.port, InetAddress.getByName("localhost"));
		this.node_id = (long) (Random.class.newInstance().nextDouble() * 128L);
		start();
	}

	public void sendm() throws IOException, JSONException {
		byte[] buffer = new byte[1024];
		InetAddress IPAddress = InetAddress.getByName("localhost");
		JSONObject jo = new JSONObject();
		jo.put("THIS", "this is a test UDP message send");
		buffer = jo.toString().getBytes();
		DatagramPacket sendp = new DatagramPacket(buffer, buffer.length, IPAddress, 8787);

		this.socket.send(sendp);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see P2P.PeerSearchSimplified#joinNetwork(java.net.InetSocketAddress, java.lang.String, java.lang.String) Join Network
	 */
	@Override
	public synchronized long joinNetwork(InetAddress inetAddress, String identifier, String target_identifier) throws JSONException, IOException, InterruptedException {

		this.node_id = Integer.parseInt(identifier);
		// DatagramSocket join = new DatagramSocket(this.port);
		JSONObject jsonjoin = new JSONObject();
		jsonjoin.put("type", "JOINING_NETWORK_SIMPLIFIED");
		jsonjoin.put("node_id", identifier);
		jsonjoin.put("target_id", target_identifier);
		jsonjoin.put("ip_address", InetAddress.getLocalHost().toString());
		byte[] buffer = new byte[1024];
		buffer = jsonjoin.toString().getBytes();
		InetAddress IPAddress = InetAddress.getByName("localhost");
		DatagramPacket sendp = new DatagramPacket(buffer, buffer.length, IPAddress, 8787);
		this.socket.send(sendp);

		return 1;
	}

	@SuppressWarnings("resource")
	@Override
	public boolean leaveNetwork(long network_id) throws IOException {
		JSONObject jsonleave = new JSONObject();
		try {
			jsonleave.put("type", "LEAVING_NETWORK");
			jsonleave.put("node_id", network_id);
			byte[] buf = new byte[1024];
			buf = jsonleave.toString().getBytes("UTF8");

			Iterator<Map.Entry<String, String>> it = routingTable.entrySet().iterator(); // it object to go through routing table
			DatagramSocket socket = new DatagramSocket(8787); // UDP socket, no
																// end to end
																// connection :)

			while (it.hasNext()) { // send leaving message to all nodes in the
									// routing table
				Map.Entry<String, String> ip = it.next();
				DatagramPacket sendp = new DatagramPacket(buf, buf.length, InetAddress.getByName(ip.getValue()), Integer.parseInt(ip.getKey()));
				socket.send(sendp);
			}
			return true; // success!
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public void indexPage(String url, String[] unique_words) {

		int[] hashes = new int[unique_words.length];
		for (int i = 0; i < unique_words.length; i++) {
			JSONObject index = new JSONObject();
			try {
				hashes = hashWords(unique_words);
				index.put("type", "INDEX");
				index.put("target_id", String.valueOf(this.node_id));
				index.put("keyword", hashes[i]); // hashed word to send
				JSONArray wordarr = new JSONArray();
				wordarr.put(url);
				index.put("link", wordarr);
				byte[] buf = new byte[1024];
				buf = index.toString().getBytes();
				InetAddress IPAddress = InetAddress.getByName("localhost");
				int p = routingTableLookUp(hashes[i]); // hash for matches the node that should contain it
				DatagramPacket dp = new DatagramPacket(buf, buf.length, IPAddress, p);
				this.socket.send(dp);
				

			} catch (JSONException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}



	private int[] hashWords(String[] unique_words) {
		int[] hashes = new int[unique_words.length];
		for (int i = 0; i < hashes.length; i++) {
			for (int j = 0; j < unique_words[i].length(); j++) {
				hashes[i] = hashes[i] * 31 + unique_words[i].charAt(j);
			}
			hashes[i] = Math.abs(hashes[i]);
		}
		return hashes;
	}

	@Override
	public SearchResult[] search(String[] words) throws JSONException, InterruptedException {
		int[] hashes = new int[words.length];
		hashes = hashWords(words);
		ArrayList<String> r = new ArrayList<String>();
		
		for (int i = 0; i < words.length; i++) {

			JSONObject jsonsearch = new JSONObject();
			jsonsearch.put("type", "SEARCH");
			jsonsearch.put("word", words);
			jsonsearch.put("node_id", String.valueOf(hashes[i]));
			jsonsearch.put("sender_id", String.valueOf(this.node_id));
			byte[] buf = new byte[1024];
			byte[] rec = new byte[1024];
			buf = jsonsearch.toString().getBytes();
			try {
				DatagramPacket dp = new DatagramPacket(buf, buf.length, InetAddress.getByName("localhost"), 8788);
				DatagramPacket rp = new DatagramPacket(rec, rec.length); // receive packet
				this.socket.send(dp);
				this.socket.receive(rp);
					this.socket.setSoTimeout(3000); // 3 seconds till timeout
					this.socket.receive(rp);

				if(rp.getData().toString().equals(null))
					ping(jsonsearch); // ping node that could be dead!
				else {
					JSONArray temparr = new JSONArray();
					String data = rp.getData().toString();
					JSONObject result = new JSONObject(data);
					temparr = result.getJSONArray("response");
					for(int j = 0; j < temparr.length(); j++ ) {
						r.add(temparr.getString(j));
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		return (SearchResult[]) r.toArray(); // return results
	}
	
	private void ping(JSONObject search_res) throws InterruptedException {
		JSONObject ping_n = new JSONObject();
		try {
			ping_n.put("type", "PING");
			ping_n.put("target_id", search_res.get("node_id").toString());
			ping_n.put("sender_id", String.valueOf(this.node_id));
			ping_n.put("ip_address", InetAddress.getByName("localhost").toString());
			byte[] buf= new byte[1024];
			byte[] rec = new byte[1024];
			buf = ping_n.toString().getBytes();
			DatagramPacket dp = new DatagramPacket(buf, buf.length, InetAddress.getByName("localhost"), 8788);
			DatagramPacket rp = new DatagramPacket(rec, rec.length); // receive packet
			this.socket.send(dp);
			this.socket.setSoTimeout(10000); // 10 seconds before timeout
			//this.socket.setSoTimeout(10000);
			this.socket.receive(rp); // ACK arrived
			String ackno = rp.getData().toString();
			if(ackno.length() < 1){
				routingTable.remove(search_res.get("node_id").toString()); // removed dead node from rtable
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public void run() {
		while (true) {
			try {
				byte[] buf = new byte[1024];
				DatagramPacket rec = new DatagramPacket(buf, buf.length);
				this.socket.receive(rec);
				String sen = new String(rec.getData());
				if (sen.length() > 0) {
					JSONObject jsonparse = new JSONObject(sen);
					sen = jsonparse.get("type").toString();
					switch (sen) {
					case "JOINING_NETWORK_SIMPLIFIED":
						try {
							respond_join(jsonparse);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						break;
					case "JOIN_NETWORK_RELAY": // can use same function as routing information since implementing simpler version of project
						routing_info(jsonparse);
						break;
					case "PING":
						ACK(jsonparse);
						break;
					case "ROUTING_INFO":
						routing_info(jsonparse);
						break;
					case "LEAVING_NETWORK":
						removeNodeFromRoutingTable(jsonparse);
						break;
					case "INDEX":
						handleIndexing(jsonparse);
						ACK(jsonparse); // acknowledge index request
						break;
					case "SEARCH":
						searchRespond(jsonparse);
						break;
					default:
						System.out.println("No matching type found");
						break;
					}

				}

			} catch (IOException e) {
				System.out.println("Socket error!");
				e.printStackTrace();
			} catch (JSONException e) {
				System.out.println("JSON error!");
				e.printStackTrace();
			}
			try {
				sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void searchRespond(JSONObject jsonparse) throws JSONException {
		JSONObject search_response = new JSONObject();
		search_response.put("type", "SEARCH_RESPONSE");
		search_response.put("word", jsonparse.get("word").toString());
		search_response.put("node_id", String.valueOf(this.node_id));
		search_response.put("sender_id", jsonparse.get("sender_id").toString());
		JSONArray resp = new JSONArray();
		indexing.get(jsonparse.get("word").toString());
		Iterator<Map.Entry<String, String>> it = routingTable.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> data = it.next();
			String url = data.getKey();
			JSONObject url1 = new JSONObject(url);
			
			resp.put(url1);
			JSONObject rank = new JSONObject(data.getValue());
			resp.put(rank);
		}
		search_response.put("response", resp);

	}

	/*
	 * This function takes in a json file gets the links for the hashed word makes sure the word is in the nodes database makes sure link is not already in the database adds information
	 */
	private void handleIndexing(JSONObject jsonparse) {

		ArrayList<String> temp = new ArrayList<String>();
		JSONArray arr = new JSONArray();
		try {
			String word = jsonparse.getString("keyword").toString();
			arr = (JSONArray) jsonparse.get("link");
			for (int i = 0; i < arr.length(); i++) {
				JSONObject o = arr.getJSONObject(i);
				String link = o.toString();
				if (indexing.containsKey(word)) {
					if (!indexing.get(word).containsValue(link)) {
						indexing.get(word).put(link, 0); // the lower the rank, less hits
					} else { 							
						Integer rank = indexing.get(word).get(link);
						rank++;
						indexing.get(word).put(link, rank);	// link exists, update rank					
					}
				} else {
					temp.add(link);
					indexing.put(word, null); // add new word and link
					HashMap<String, Integer> lr = new HashMap<String,Integer>();
					lr.put(link, 0);
					indexing.get(word).putAll(lr);
				}
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void ACK(JSONObject jsonparse) throws JSONException, IOException {
		JSONObject ack = new JSONObject();
		ack.put("type", "ACK");
		ack.put("node_id", String.valueOf(this.node_id));
		ack.put("ip_address", jsonparse.get("ip_address"));
		byte[] buf = new byte[1024];
		buf = ack.toString().getBytes();
		InetAddress IPAddress = InetAddress.getByName("localhost");
		DatagramPacket dp = new DatagramPacket(buf, buf.length, IPAddress, 8787);
		this.socket.send(dp);
	}

	private void removeNodeFromRoutingTable(JSONObject json) throws JSONException {
		routingTable.remove(json.get("id_node").toString());

	}

	/*
	 * After a node receives a message with ROUTING INFO, this method is called Here the node will update its routing table with new nodes information.
	 */
	private void routing_info(JSONObject jsonparser) throws JSONException {
		JSONArray arr = (JSONArray) jsonparser.get("routing_table");
		for (int i = 0; i < arr.length(); i++) {
			JSONObject temp = arr.getJSONObject(i);
			if (!this.routingTable.containsKey(temp.get("node_id").toString())) // no duplication
				this.routingTable.put(temp.get("node_id").toString(), temp.get("ip_address").toString());
		}
	}
	/*
	 * Not used in this case
	 */
	@SuppressWarnings("unused")
	private void relay_join(JSONObject jsonparse) {
		
	}

	private synchronized void respond_join(JSONObject jsonparse) throws JSONException, IOException, InterruptedException {
		routingTable.put(jsonparse.get("node_id").toString(), InetAddress.getByName("localhost").toString()); // addin joining node to gateway
		Iterator<Map.Entry<String, String>> it = routingTable.entrySet().iterator(); // it object to go through routing table
		JSONObject rouinfo = new JSONObject(); // will store the array of node:ips
		rouinfo.put("type", "ROUTING_INFO");
		rouinfo.put("gateway_id", String.valueOf(this.node_id)); // gateway node id
		rouinfo.put("node_id", jsonparse.get("node_id")); // indicates target node (which is the joining node)
		rouinfo.put("ip_address", InetAddress.getByName("localhost").toString());
		JSONArray table = new JSONArray(); // building json array for routing table

		while (it.hasNext()) {
			Map.Entry<String, String> ip = it.next();
			JSONObject tmp = new JSONObject();
			tmp.put("node_id", ip.getKey());
			tmp.put("ip_address", ip.getValue());
			int node_relay = Integer.parseInt(ip.getKey().toString());
			/*
			 * Find a Node numerically closer to incoming node in gateways route table
			 * route message onto numerically closer node
			 */
			if(Math.abs(Math.min(Integer.parseInt(jsonparse.getString("node_id")) - node_relay, Integer.parseInt(jsonparse.getString("node_id")) - this.node_id)) == node_relay) {
				byte[] bugrel = new byte[1024];
				JSONObject netrel = new JSONObject();
				netrel.put("type", "JOINING_NETWORK_RELAY");
				netrel.put("node_id", jsonparse.getString("node_id"));
				netrel.put("gateway", String.valueOf(this.node_id));
				DatagramPacket relay = new DatagramPacket(bugrel, bugrel.length,InetAddress.getByName("localhost"),8789);
				this.socket.send(relay);
				
			}
			table.put(tmp);
		}
		rouinfo.put("routing_table", table);
		byte[] buf = new byte[1024];
		buf = rouinfo.toString().getBytes();
		InetAddress IPAddress = InetAddress.getByName("localhost");
		DatagramPacket sendRoutingInfo = new DatagramPacket(buf, buf.length, IPAddress, 8788);
		this.socket.send(sendRoutingInfo);
		sleep(1000);
	}

	int routingTableLookUp(int id) {
		return Integer.parseInt(this.routingTable.get(id));
	}

}
