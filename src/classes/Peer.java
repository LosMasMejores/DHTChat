package classes;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class Peer implements Runnable {
	static final int k = 2;
	static final int alpha = 3;
	static final int bucketLength = 160;

	byte[] myGuid;
	byte[] getNode;
	Map<String, String> hashTable;
	byte[][][] kBucket;
	String getValue;

	String ip;
	int port;
	InetAddress group;
	MulticastSocket socket;
	Semaphore getNodeSem;
	Semaphore getValueSem;

	public Peer(String identificador, String ip, int port) {
		this.myGuid = sha1(identificador);
		this.getNode = sha1(identificador);
		this.hashTable = new HashMap<>();
		this.kBucket = new byte[bucketLength][k][];
		this.kBucket[bucketLength - 1][0] = this.myGuid;
		this.getValue = "";
		this.ip = ip;
		this.port = port;
		this.getNodeSem = new Semaphore(1, true);
		this.getValueSem = new Semaphore(1, true);

		try {
			this.group = InetAddress.getByName(this.ip);
			this.socket = new MulticastSocket(this.port);
			// this.socket.setLoopbackMode(true);
			this.socket.joinGroup(this.group);
			this.getNodeSem.acquire();
			this.getValueSem.acquire();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized byte[] put(byte[] key, String value) {
		System.out.println(Base64.getEncoder().encodeToString(myGuid) + " start putting");

		byte[] guid = getNode(key);

		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#node#"
					+ Base64.getEncoder().encodeToString(guid) + "#Q#" + Base64.getEncoder().encodeToString(key)
					+ "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
			getNodeSem.acquire();
			guid = getNode;
			getNode = myGuid;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#put#" + Base64.getEncoder().encodeToString(guid)
					+ "#" + Base64.getEncoder().encodeToString(key) + "#" + value + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return guid;
	}

	public synchronized String get(byte[] key) {
		System.out.println(Base64.getEncoder().encodeToString(myGuid) + " start getting");

		String value = "";
		byte[] guid = getNode(key);

		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#node#"
					+ Base64.getEncoder().encodeToString(guid) + "#Q#" + Base64.getEncoder().encodeToString(key)
					+ "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
			getNodeSem.acquire();
			guid = getNode;
			getNode = myGuid;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#get#" + Base64.getEncoder().encodeToString(guid)
					+ "#Q#" + Base64.getEncoder().encodeToString(key) + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
			getValueSem.acquire();
			value = getValue;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		return value;
	}

	private int distance(byte[] key, byte[] guid) {
		int d = -1;

		if (key.length != guid.length) {
			return d;
		}

		BitSet keyBit = BitSet.valueOf(key);
		BitSet guidBit = BitSet.valueOf(guid);

		for (int i = 0; i < 160; i++) {
			d++;
			if (keyBit.get(i) != guidBit.get(i)) {
				break;
			}
		}

		return d;
	}

	public static byte[] sha1(String value) {
		byte[] key = null;

		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(value.getBytes());
			key = md.digest();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return key;
	}

	private byte[] getNode(byte[] key) {
		byte[] guid = null;
		int d = distance(key, myGuid);

		while (guid == null) {
			guid = kBucket[d][0];
			if (guid == null) {
				guid = kBucket[d][1];
			}
			d++;
		}

		return guid;
	}

	private void putIntoBucket(byte[] guid) {
		int d = distance(guid, myGuid);

		for (int i = 0; i < k; i++) {
			if (Arrays.equals(kBucket[d][i], guid)) {
				return;
			}
		}

		for (int i = 0; i < k; i++) {
			if (kBucket[d][i] == null) {
				kBucket[d][i] = guid;
				break;
			}
		}

		return;
	}

	private void getOutOfBucket(byte[] guid) {
		int d = distance(guid, myGuid);

		for (int i = 0; i < k; i++) {
			if (Arrays.equals(kBucket[d][i], guid)) {
				kBucket[d][i] = null;
				break;
			}
		}

		return;
	}

	public void run() {
		byte[] prevNode = myGuid;

		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#hi" + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		byte[] buf = new byte[1024];

		while (true) {
			DatagramPacket recv = new DatagramPacket(buf, buf.length);

			try {
				socket.receive(recv);
				String cmd[] = (new String(recv.getData())).split("#");
				// [guid_sender]#[command]#[guid_reciver]#[arguments:...]#end

				if (cmd.length < 2) {
					continue;
				}

				switch (cmd[1]) {
				case "hi":
					if (Arrays.equals(Base64.getDecoder().decode(cmd[0]), myGuid)) {
						break;
					}
					System.out.println(Base64.getEncoder().encodeToString(myGuid) + " gets hi from " + cmd[0]);
					putIntoBucket(Base64.getDecoder().decode(cmd[0]));
					String msg = Base64.getEncoder().encodeToString(myGuid) + "#sup" + "#end";
					socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
					for (Map.Entry<String, String> entry : hashTable.entrySet()) {
						byte[] key = Base64.getDecoder().decode(entry.getKey());
						if (distance(myGuid, key) <= distance(Base64.getDecoder().decode(cmd[0]), key)) {
							try {
								msg = Base64.getEncoder().encodeToString(myGuid) + "#put#" + cmd[0] + "#"
										+ entry.getKey() + "#" + entry.getValue() + "#end";
								socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					break;

				case "sup":
					if (Arrays.equals(Base64.getDecoder().decode(cmd[0]), myGuid)) {
						break;
					}
					System.out.println(Base64.getEncoder().encodeToString(myGuid) + " gets sup from " + cmd[0]);
					putIntoBucket(Base64.getDecoder().decode(cmd[0]));
					break;

				case "bye":
					System.out.println(Base64.getEncoder().encodeToString(myGuid) + " gets bye from " + cmd[0]);
					getOutOfBucket(Base64.getDecoder().decode(cmd[0]));
					break;

				case "put":
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}

					hashTable.put(cmd[3], cmd[4]);
					System.out.println(Base64.getEncoder().encodeToString(myGuid) + " is putting "
							+ hashTable.get(cmd[3]) + " in " + cmd[3]);
					break;

				case "get":
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}

					switch (cmd[3]) {
					case "Q":
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " asked for value");
						String value = hashTable.get(cmd[4]);
						if (value == null) {
							value = "";
						}
						try {
							msg = Base64.getEncoder().encodeToString(myGuid) + "#get#" + cmd[0] + "#A#" + value
									+ "#end";
							socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
						} catch (IOException e) {
							e.printStackTrace();
						}
						break;

					case "A":
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " get the value");
						getValue = cmd[4];
						getValueSem.release();
						break;

					default:
						break;
					}

					break;

				case "node":
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}
					switch (cmd[3]) {
					case "Q":
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " asked for node");
						byte[] node = getNode(Base64.getDecoder().decode(cmd[4]));
						try {
							msg = Base64.getEncoder().encodeToString(myGuid) + "#node#" + cmd[0] + "#A#" + cmd[4] + "#"
									+ Base64.getEncoder().encodeToString(node) + "#end";
							socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
						} catch (IOException e) {
							e.printStackTrace();
						}
						break;

					case "A":
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " get the node");
						if (Arrays.equals(getNode, Base64.getDecoder().decode(cmd[5]))) {
							getNodeSem.release();
							break;
						}

						if (Arrays.equals(prevNode, Base64.getDecoder().decode(cmd[5]))) {
							getNode = prevNode;
							prevNode = myGuid;
							getNodeSem.release();
							break;
						}

						prevNode = Base64.getDecoder().decode(cmd[0]);
						getNode = Base64.getDecoder().decode(cmd[5]);

						try {
							msg = Base64.getEncoder().encodeToString(myGuid) + "#node#" + cmd[5] + "#Q#" + cmd[4]
									+ "#end";
							socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
						} catch (IOException e) {
							e.printStackTrace();
						}
						break;

					default:
						break;

					}
					break;

				default:
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
				break;
			}
		}

		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#bye" + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
}
