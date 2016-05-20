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
	static final int bucketLength = 160;

	/*
	 * Variable para DHT
	 */
	byte[] myGuid;
	byte[] getNode;
	Map<String, String> hashTable;
	byte[][][] kBucket;
	String getValue;

	/*
	 * Variables para socket multicast
	 */
	String ip;
	int port;
	InetAddress group;
	MulticastSocket socket;

	/*
	 * Variables para control de acceso
	 */
	Semaphore getNodeSem;
	Semaphore getValueSem;

	boolean debug = false;

	public Peer(String identificador, String ip, int port) {
		this.myGuid = sha1(identificador);
		this.getNode = sha1(identificador);
		this.hashTable = new HashMap<>();
		this.kBucket = new byte[bucketLength][k][];
		this.getValue = "";
		this.ip = ip;
		this.port = port;
		this.getNodeSem = new Semaphore(1, true);
		this.getValueSem = new Semaphore(1, true);

		try {
			this.group = InetAddress.getByName(this.ip);
			this.socket = new MulticastSocket(this.port);
			/*
			 * Descomentar para no recibir mensajes del loopback
			 */
			// this.socket.setLoopbackMode(true);
			this.socket.joinGroup(this.group);
			this.getNodeSem.acquire();
			this.getValueSem.acquire();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public Peer(String identificador, String ip, int port, boolean debug) {
		this.myGuid = sha1(identificador);
		this.getNode = sha1(identificador);
		this.hashTable = new HashMap<>();
		this.kBucket = new byte[bucketLength][k][];
		this.getValue = "";
		this.ip = ip;
		this.port = port;
		this.getNodeSem = new Semaphore(1, true);
		this.getValueSem = new Semaphore(1, true);

		this.debug = debug;

		try {
			this.group = InetAddress.getByName(this.ip);
			this.socket = new MulticastSocket(this.port);
			/*
			 * Descomentar para no recibir mensajes del loopback
			 */
			// this.socket.setLoopbackMode(true);
			this.socket.joinGroup(this.group);
			this.getNodeSem.acquire();
			this.getValueSem.acquire();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Metodo para alojar un valor en su nodo mas cercano
	 */
	public synchronized byte[] put(byte[] key, String value) {
		if (debug)
			System.out.println(Base64.getEncoder().encodeToString(myGuid) + " start putting");

		byte[] guid = getNode(key);

		/*
		 * Conseguimos el nodo mas cercano a la clave
		 */
		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#node#"
					+ Base64.getEncoder().encodeToString(guid) + "#Q#" + Base64.getEncoder().encodeToString(key)
					+ "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
			// Esperar por respuesta
			getNodeSem.acquire();
			guid = getNode;
			getNode = myGuid;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		/*
		 * Enviamos al nodo el mensaje PUT
		 */
		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#put#" + Base64.getEncoder().encodeToString(guid)
					+ "#" + Base64.getEncoder().encodeToString(key) + "#" + value + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return guid;
	}

	/*
	 * Metodo para conseguir un valor de su nodo mas cercano
	 */
	public synchronized String get(byte[] key) {
		if (debug)
			System.out.println(Base64.getEncoder().encodeToString(myGuid) + " start getting");

		String value = "";
		byte[] guid = getNode(key);

		/*
		 * Conseguimos el nodo mas cercano a la clave
		 */
		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#node#"
					+ Base64.getEncoder().encodeToString(guid) + "#Q#" + Base64.getEncoder().encodeToString(key)
					+ "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
			// Esperar por respuesta
			getNodeSem.acquire();
			guid = getNode;
			getNode = myGuid;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		/*
		 * Enviamos al nodo el mensaje GET
		 */
		try {
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#get#" + Base64.getEncoder().encodeToString(guid)
					+ "#Q#" + Base64.getEncoder().encodeToString(key) + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
			// Esperar por respuesta
			getValueSem.acquire();
			value = getValue;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		return value;
	}

	/*
	 * Metodo para calcular la distancia a una clave
	 */
	private int distance(byte[] key, byte[] guid) {
		int d = -1;

		if (key.length != guid.length) {
			return d;
		}

		// Pasar de byte a bit
		BitSet keyBit = BitSet.valueOf(key);
		BitSet guidBit = BitSet.valueOf(guid);

		for (d = 0; d < 160; d++) {
			if (keyBit.get(d) != guidBit.get(d)) {
				break;
			}
		}

		return d;
	}

	/*
	 * Metodo para resumir un valor
	 */
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

	/*
	 * Metodo para conseguir el nodo mas cercano a una clave
	 */
	private byte[] getNode(byte[] key) {
		byte[] guid = null;
		int d = distance(key, myGuid);

		while (guid == null && d < 160) {
			guid = kBucket[d][0];
			if (guid == null) {
				guid = kBucket[d][1];
			}
			d++;
		}

		if (d == 160) {
			return myGuid;
		}

		return guid;
	}

	/*
	 * Metodo para poner en el kbucket un nodo
	 */
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

	/*
	 * Metodo para quitar del kbucket un nodo
	 */
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

	/*
	 * Metodo principal de escucha en el grupo multicast
	 */
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
				// Recibir mensaje de saludo
				case "hi":
					if (Arrays.equals(Base64.getDecoder().decode(cmd[0]), myGuid)) {
						break;
					}
					if (debug)
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " gets hi from " + cmd[0]);
					putIntoBucket(Base64.getDecoder().decode(cmd[0]));
					String msg = Base64.getEncoder().encodeToString(myGuid) + "#sup#" + cmd[0] + "#end";
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

				// Recibir mensaje de actualizacion
				case "sup":
					if (Arrays.equals(Base64.getDecoder().decode(cmd[0]), myGuid)) {
						break;
					}
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}
					if (debug)
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " gets sup from " + cmd[0]);
					putIntoBucket(Base64.getDecoder().decode(cmd[0]));
					break;

				// Recibir mensaje de despido
				case "bye":
					if (debug)
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " gets bye from " + cmd[0]);
					getOutOfBucket(Base64.getDecoder().decode(cmd[0]));
					break;

				// Recibir mensaje de put
				case "put":
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}

					hashTable.put(cmd[3], cmd[4]);
					if (debug)
						System.out.println(Base64.getEncoder().encodeToString(myGuid) + " is putting "
								+ hashTable.get(cmd[3]) + " in " + cmd[3]);
					break;

				// Recibir mensaje de get
				case "get":
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}

					switch (cmd[3]) {
					// si nos preguntan...
					case "Q":
						if (debug)
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
					// si nos contestan...
					case "A":
						if (debug)
							System.out.println(Base64.getEncoder().encodeToString(myGuid) + " get the value");
						getValue = cmd[4];
						getValueSem.release();
						break;

					default:
						break;
					}

					break;

				// Recibir mensaje de node(nodo mas cercano)
				case "node":
					if (!Arrays.equals(Base64.getDecoder().decode(cmd[2]), myGuid)) {
						break;
					}
					switch (cmd[3]) {
					// si nos preguntan...
					case "Q":
						if (debug)
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

					// si nos contestan...
					case "A":
						if (debug)
							System.out.println(Base64.getEncoder().encodeToString(myGuid) + " get the node");
						// Dos veces la misma respuesta
						if (Arrays.equals(getNode, Base64.getDecoder().decode(cmd[5]))) {
							getNodeSem.release();
							break;
						}

						// Problema del bucle
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
			// Nos despedimos
			String msg = Base64.getEncoder().encodeToString(myGuid) + "#bye" + "#end";
			socket.send(new DatagramPacket(msg.getBytes(), msg.getBytes().length, group, port));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
}
