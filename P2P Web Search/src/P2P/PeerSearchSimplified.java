package P2P;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;

import org.json.JSONException;

public interface PeerSearchSimplified  {
	DatagramSocket init(DatagramSocket udp_socket) throws SocketException, UnknownHostException;
	long joinNetwork(InetAddress bootstrap_node, String identifier, String target_identifier ) throws JSONException, IOException, InterruptedException; //returns network_id, a locally 
	boolean leaveNetwork(long network_id)throws IOException; // parameter is previously returned peer network 
	void indexPage(String url, String[] unique_words);
	SearchResult[] search(String[] words) throws JSONException, InterruptedException;
	void init(int udp_socket) throws SocketException, UnknownHostException, InstantiationException, IllegalAccessException;
}
