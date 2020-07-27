package com.grupo12.aerospike;

import java.util.Scanner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;
import com.grupo12.aerospike.servicios.ServicioUsuario;

public class Demo {
	private String seedHost;
	private int port;
	private AerospikeClient cliente;

	public Demo() {
		ClientPolicy clientPolicy = new ClientPolicy();
		this.cliente = new AerospikeClient(clientPolicy);
	}

	public Demo(String seedHost, int port) {
		ClientPolicy clientPolicy = new ClientPolicy();
		this.seedHost = seedHost;
		this.port = port;
		this.cliente = new AerospikeClient(clientPolicy, seedHost, port);
	}

	protected void finalize() throws Throwable {
		if (this.cliente != null) {
			this.cliente.close();
		}
	}

	protected void work() {
		try {
			System.out.println("INFO: Conectándose a Aerospike");

			// Estableciendo conexión con Aerospike
			if (cliente == null || !cliente.isConnected()) {
				System.out.println("\nERROR: Fallo en la conexión");
			} else {
				Scanner input = new Scanner(System.in);
				System.out.println("\nINFO: Conexión a Aerospike exitosa");

				// Inicializar servicio
				ServicioUsuario su = new ServicioUsuario(cliente);

				// Opciones
				int seleccion = 1;
				while (seleccion != 0) {
					System.out.println("\nIngrese la acción que desea realizar: \n");
					System.out.println("1> Cargar usuario manualmente\n");
					System.out.println("2> Cargar usuarios desde un archivo .csv\n");
					System.out.println("3> Leer el registro de un usuario\n");
					System.out.println("4> Leer todos los registros de usuarios\n");
					System.out.println("5> Eliminar el registro de un usuario\n");
					System.out.println("6> Eliminar todos los registros de usuarios\n");
					System.out.println("7> Mostrar usuarios masculinos con una cantidad de twits entre 10.000 y 20.000\n");
					System.out.println("8> Mostrar usuarios mayores de edad interesados unicamente en deportes\n");
					System.out.println("9> Mostrar usuarios que no estan interesados en musica y arte\n");
					System.out.println("10> Mostrar a cuantos usuarios les interesa cada categoria de intereses\n");
					System.out.println("11> Mostrar registro de usuario femenino con mayor numero de tweets\n");
					System.out.println("0> Salir\n");
					System.out.println("\nSeleccione 0-11 y presione Enter\n");
					seleccion = input.nextInt();

					switch (seleccion) {
					case 1:
						System.out.println("\n**********Ha seleccionado: Cargar usuario manualmente**********\n");
						su.crearUsuario();
						break;
					case 2:
						System.out.println(
								"\n**********Ha seleccionado: Cargar usuarios desde un archivo .csv**********\n");
						su.insertarUsuariosDesdeCsv();
						break;
					case 3:
						System.out.println("\n**********Ha seleccionado: Leer el registro de un usuario**********\n");
						su.mostrarUsuario();
						break;
					case 4:
						System.out.println(
								"\n**********Ha seleccionado: Leer todos los registros de usuarios**********\n");
						su.mostrarTodos();
						break;
					case 5:
						System.out
								.println("\n**********Ha seleccionado: Eliminar el registro de un usuario**********\n");
						su.borrarUsuario();
						break;
					case 6:
						System.out.println(
								"\n**********Ha seleccionado: Eliminar todos los registros de usuarios**********\n");
						su.borrarTodos();
						break;
					case 7:
						System.out.println("\n**********Ha seleccionado: Mostrar usuarios masculinos con una cantidad de twits entre 10.000 y 20.000**********\n");
						su.consultaInteresante1();
						break;
					case 8:
						System.out.println("\n**********Ha seleccionado: Mostrar usuarios mayores de edad interesados unicamente en deportes**********\n");
						su.consultaInteresante2();
						break;
					case 9:
						System.out.println("\n**********Ha seleccionado: Mostrar usuarios que no estan interesados en musica y arte**********\n");
						su.consultaInteresante3();
						break;
					case 10:
						System.out.println("\n**********Ha seleccionado: Mostrar a cuantos usuarios les interesa cada categoria de intereses**********\n");
						su.consultaInteresante4();
						break;
					case 11:
						System.out.println("\n**********Ha seleccionado: Mostrar registro de usuario femenino con mayor numero de tweets**********\n");
						su.consultaInteresante5();
						break;
					default:
						break;
					}
				}
			}
		} catch (AerospikeException e) {
			System.out.println("Aerospike Exception - Mensaje: " + e.getMessage() + "\n");
			System.out.println("Aerospike Exception - Stack Trace: " + e.getStackTrace() + "\n");
		} catch (Exception e) {
			System.out.println("Exception - Mensaje: " + e.getMessage() + "\n");
			System.out.println("Exception - Stack Trace: " + e.getStackTrace() + "\n");
		} finally {
			if (cliente != null && cliente.isConnected()) {
				cliente.close();
			}
			System.out.println("\nINFO: Saliendo de la aplicación...");
			System.out.println("\nINFO: Gracias por su atención!");
		}
	}

	public String getSeedHost() {
		return seedHost;
	}

	public void setSeedHost(String seedHost) {
		this.seedHost = seedHost;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public AerospikeClient getCliente() {
		return cliente;
	}

	public void setCliente(AerospikeClient cliente) {
		this.cliente = cliente;
	}

	public static void main(String[] args) {
		Demo demo = new Demo("172.28.128.4", 3000);
		demo.work();

	}

}
