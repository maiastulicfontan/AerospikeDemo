package com.grupo12.aerospike.servicios;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class ServicioUsuario {
	private AerospikeClient cliente;
	
	public ServicioUsuario() {
		
	}
	
	public ServicioUsuario (AerospikeClient cliente) {
		this.cliente = cliente;
	}
	
	public void crearUsuario() throws AerospikeException {
		System.out.println("\n**************Crear Usuario**************\n");
		
		String nombreUsuario;
		String password;
		String genero;
		String fechaNacimiento; //cambiar por Date?
		String intereses;
		
		Scanner input = new Scanner(System.in);
		
		System.out.println("Ingrese nombre de usuario: ");
		nombreUsuario = input.nextLine();
		
		if (nombreUsuario != null && nombreUsuario.length() > 0) {
			System.out.println("Ingrese contraseña para el usuario "+ nombreUsuario + ":");
			password = input.nextLine();
			
			System.out.println("Ingrese genero (M-Masculino, F-Femenino, O-Otro): ");
			genero = input.nextLine().substring(0, 1);
			
			System.out.println("Ingrese fecha de nacimiento con formato YYYY-mm-dd: ");
			fechaNacimiento = input.nextLine();
			
			System.out.println("Ingrese intereses separados por comas: ");
			intereses = input.nextLine();
			
			insertarUsuario(nombreUsuario, password, genero, fechaNacimiento, 0, intereses);
			
			System.out.println("\nINFO: Registro de usuario creado");
		}
	}
	
	public void insertarUsuario(String nombreUsuario, String password, String genero, String fechaNacimiento, int contadorTweets, String intereses ) {
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		
		Key key = new Key("test", "usuarios", nombreUsuario);
		Bin bin1 = new Bin("nom_usuario", nombreUsuario);
		Bin bin2 = new Bin("password", password);
		Bin bin3 = new Bin("genero", genero);
		Bin bin4 = new Bin("fecha_nac", fechaNacimiento);
		Bin bin5 = new Bin("cont_tweets", contadorTweets);
		Bin bin6 = new Bin("intereses", Arrays.asList(intereses.split(",")));
		
		cliente.put(writePolicy, key, bin1, bin2, bin3, bin4, bin5, bin6);		
	}
	
	
	public void insertarUsuariosDesdeCsv (String pathArchivoCsv) {
		try {
			Reader in = new FileReader(pathArchivoCsv);
			Iterable<CSVRecord> registros = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
			int contadorRegistros = 0;
			for (CSVRecord registro : registros) {
				String nombreUsuario = registro.get("nombre_usuario");
				String password = registro.get("password");
				String genero = registro.get("genero");
				String fechaNacimiento = registro.get("fecha_nacimiento");
				int contadorTweets = Integer.parseInt(registro.get("contador_tweets"));
				String intereses = registro.get("intereses");
				insertarUsuario(nombreUsuario, password, genero, fechaNacimiento, contadorTweets, intereses);
				contadorRegistros++;
			}
			System.out.println("Se han insertado "+contadorRegistros+" registros.");
		} catch (FileNotFoundException e) {
			System.out.println("File not found exception - Stack Trace: "+e.getStackTrace());
		} catch (IOException e) {
			System.out.println("IO exception - Stack Trace: "+e.getStackTrace());
		}
	}
	
	public void printUsuario(String nombreUsuario, String password, String genero, String fechaNacimiento, int contadorTweets, String intereses ) {
		System.out.println(nombreUsuario+","+password+","+genero+","+fechaNacimiento+","+contadorTweets+","+intereses);
	}
}
