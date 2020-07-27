package com.grupo12.aerospike.servicios;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

public class ServicioUsuario {
	public static int contadorR;
	public static int max_tweets;
	public static String nombreConsultaI5;
	public static String passwordConsultaI5;
	public static String generoConsultaI5;
	public static String fecha_nacConsultaI5;
	public static String cont_tweetsConsultaI5;
	public static String interesesConsultaI5;
	public static int acuDeportes;
	public static int acuLiteratura;
	public static int acuTecnologia;
	public static int acuFinanzas;
	public static int acuFotografia;
	public static int acuMusica;
	public static int acuArte;
	private AerospikeClient cliente;
	private long contadorRegistros;
	private Set<String> keys = new LinkedHashSet<String>();

	public ServicioUsuario() {
		contadorRegistros = 0;
	}

	public ServicioUsuario (AerospikeClient cliente) {
		this.cliente = cliente;
		contadorRegistros = 0;
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
		try {
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
			contadorRegistros++;
		} catch (AerospikeException e) {
			System.out.println("Aerospike Exception - Mensaje: "+e.getMessage());
			System.out.println("Aerospike Exception - Stack Trace: "+e.getStackTrace());
		}
	}


	public void insertarUsuariosDesdeCsv () {
		Scanner input = new Scanner(System.in);
		System.out.println("\n**************Insertando desde archivo .csv**************\n");
		System.out.println("Ingrese el path del archivo .csv");
		String pathArchivoCsv = input.nextLine();
		try {
			Reader in = new FileReader(pathArchivoCsv);
			Iterable<CSVRecord> registros = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
			int registrosInsertados = 0;
			for (CSVRecord registro : registros) {
				String nombreUsuario = registro.get("nombre_usuario");
				String password = registro.get("password");
				String genero = registro.get("genero");
				String fechaNacimiento = registro.get("fecha_nacimiento");
				int contadorTweets = Integer.parseInt(registro.get("contador_tweets"));
				String intereses = registro.get("intereses");
				insertarUsuario(nombreUsuario, password, genero, fechaNacimiento, contadorTweets, intereses);
				registrosInsertados++;
			}
			System.out.println("Se han insertado "+registrosInsertados+" registros.");
		} catch (FileNotFoundException e) {
			System.out.println("File not found exception - Stack Trace: "+e.getStackTrace());
		} catch (IOException e) {
			System.out.println("IO exception - Stack Trace: "+e.getStackTrace());
		}
	}

	public void printUsuario(String nombreUsuario, String password, String genero, String fechaNacimiento, int contadorTweets, String intereses ) {
		System.out.println(nombreUsuario+","+password+","+genero+","+fechaNacimiento+","+contadorTweets+","+intereses);
	}


	public void mostrarUsuario () throws AerospikeException{
		System.out.println("\n**************Mostrar usuario**************\n");
		Record registroUsuario = null;
		Key keyUsuario = null;
		Scanner input = new Scanner(System.in);

		String nombreUsuario;
		System.out.println("\nIngrese nombre de usuario: ");
		nombreUsuario = input.nextLine();

		if (nombreUsuario != null && nombreUsuario.length()> 0) {
			//Verificar que existe el registro
			keyUsuario = new Key("test", "usuarios", nombreUsuario);
			registroUsuario = cliente.get(null, keyUsuario);
			if (registroUsuario != null) {
				System.out.println("\nINFO: registro obtenido exitosamente. Detalles: \n");
				System.out.println("Nombre de usuario: " + registroUsuario.getValue("nom_usuario")+ "\n");
				System.out.println("Password: " + registroUsuario.getValue("password")+ "\n");
				System.out.println("Género: " + registroUsuario.getValue("genero")+ "\n");
				System.out.println("Fecha de nacimiento: " + registroUsuario.getValue("fecha_nac")+ "\n");
				System.out.println("Contador de tweets: " + registroUsuario.getValue("cont_tweets")+ "\n");
				System.out.println("Intereses: " + registroUsuario.getValue("intereses")+ "\n");
			} else {
				System.out.println("ERROR: Registro no encontrado\n");
			}
		} else {
			System.out.println("ERROR: Registro no encontrado\n");
		}
	}


	public void mostrarTodos () {
		System.out.println("\n**************Mostrando todos los usuarios**************\n");
		System.out.println("nom_usuario, password, genero, fecha_nac, cont_tweets, intereses");
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = true;
		policy.priority = Priority.LOW;
		policy.includeBinData = true;

		cliente.scanAll(policy, "test", "usuarios", new ScanCallback() {

			@Override
			public void scanCallback(Key key, Record registro) throws AerospikeException{
				keys.add(registro.getValue("nom_usuario").toString());
				System.out.println("\n"+registro.getValue("nom_usuario")+", "+registro.getValue("password")+", " +registro.getValue("genero")+", " + registro.getValue("fecha_nac")+
						", " +registro.getValue("cont_tweets")+", " +registro.getValue("intereses")+"\n");
			}
		});
		System.out.println("\nSe han encontrado "+contadorRegistros+" registros");
		this.setKeys(keys);
	}

	public void borrarUsuario() {
		System.out.println("\n**************Borrar usuario**************\n");
		Scanner input = new Scanner(System.in);
		String nombreUsuario;
		System.out.println("\nIngrese nombre de usuario: ");
		nombreUsuario = input.nextLine();
		borrarUsuarioConParametro(nombreUsuario);
		System.out.println("\nINFO: Se ha eliminado el registro del usuario "+nombreUsuario);
	}

	public void borrarUsuarioConParametro(String nombreUsuario) {
		Record registroUsuario = null;
		Key keyUsuario = null;
		WritePolicy writePolicy = new WritePolicy();
		if (nombreUsuario != null && nombreUsuario.length()> 0) {
			//Verificar que existe el registro
			keyUsuario = new Key("test", "usuarios", nombreUsuario);
			registroUsuario = cliente.get(null, keyUsuario);
			if (registroUsuario != null) {
				cliente.delete(writePolicy, keyUsuario);
				//this.getKeys().remove(nombreUsuario);
				contadorRegistros--;
			} else {
				System.out.println("\nERROR: Registro no encontrado\n");
			}
		} else {
			System.out.println("ERROR: Registro no encontrado\n");
		}
	}

	public void borrarTodos () {
		System.out.println("\n**************Borrando todos los usuarios**************\n");
		long tmpCantidadRegistros = contadorRegistros;
		this.getKeys().forEach(k ->
			borrarUsuarioConParametro(k)
		);
		System.out.println("\nSe han eliminado "+tmpCantidadRegistros+" registros");
	}

	public long getContadorRegistros() {
		return contadorRegistros;
	}

	public void setContadorRegistros(long contadorRegistros) {
		this.contadorRegistros = contadorRegistros;
	}

	public Set<String> getKeys() {
		return keys;
	}

	public void setKeys(Set<String> keys) {
		this.keys = keys;
	}
	public void consultaInteresante1(){
		System.out.println("\n**************Mostrando Aquellos usuarios masculinos con una cantidad de twits entre 10.000 y 20.000**************\n");
		System.out.println("nom_usuario, password, genero, fecha_nac, cont_tweets, intereses");
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = true;
		policy.priority = Priority.LOW;
		policy.includeBinData = true;

		cliente.scanAll(policy, "test", "usuarios", new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record registro) throws AerospikeException{
				keys.add(registro.getValue("nom_usuario").toString());
				int min_tweets = 10000;
				int max_tweets = 20000;
				if (registro.getValue("genero").equals("M")  && Integer.parseInt(registro.getValue("cont_tweets").toString()) > min_tweets && Integer.parseInt(registro.getValue("cont_tweets").toString()) < max_tweets ){

					System.out.println("\n" + registro.getValue("nom_usuario") + ", " + registro.getValue("password") + ", " + registro.getValue("genero") + ", " + registro.getValue("fecha_nac") +
							", " + registro.getValue("cont_tweets") + ", " + registro.getValue("intereses") + "\n");
					contadorR++;
				}
			}
		});
		System.out.println("\nSe han encontrado "+contadorR+" registros");
		contadorR=0;

	}

	public void consultaInteresante2(){
		System.out.println("\n**************Mostrando Todos los usuarios mayores de edad interesados unicamente en deportes**************\n");
		System.out.println("nom_usuario, password, genero, fecha_nac, cont_tweets, intereses");
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = true;
		policy.priority = Priority.LOW;
		policy.includeBinData = true;

		cliente.scanAll(policy, "test", "usuarios", new ScanCallback() {

			@Override
			public void scanCallback(Key key, Record registro) throws AerospikeException{
				keys.add(registro.getValue("nom_usuario").toString());
				LocalDate fechaNac = LocalDate.parse(registro.getValue("fecha_nac").toString());
				LocalDate fechaActual = LocalDate.now();
				long edad = ChronoUnit.YEARS.between(fechaNac, fechaActual);
				Object objectIntereses = registro.getValue("intereses");
				String stringIntereses = objectIntereses.toString();
				stringIntereses = stringIntereses.replace(" ","");
				stringIntereses = stringIntereses.replace("[","");
				stringIntereses = stringIntereses.replace("]","");
				String[] arrIntereses =  stringIntereses.split(",",0);
				if (edad>18 && arrIntereses.length == 1 && arrIntereses[0].equals("deportes") ){

					System.out.println("\n" + registro.getValue("nom_usuario") + ", " + registro.getValue("password") + ", " + registro.getValue("genero") + ", " + registro.getValue("fecha_nac") +
							", " + registro.getValue("cont_tweets") + ", " + registro.getValue("intereses") + "\n");
					contadorR++;
				}
			}
		});
		System.out.println("\nSe han encontrado "+contadorR+" registros");
		contadorR=0;

	}

	public void consultaInteresante3(){
		System.out.println("\n**************Mostrando Todos los usuarios que no estan interesados en musica y arte**************\n");
		System.out.println("nom_usuario, password, genero, fecha_nac, cont_tweets, intereses");
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = true;
		policy.priority = Priority.LOW;
		policy.includeBinData = true;

		cliente.scanAll(policy, "test", "usuarios", new ScanCallback() {

			@Override
			public void scanCallback(Key key, Record registro) throws AerospikeException{
				keys.add(registro.getValue("nom_usuario").toString());

				Object objectIntereses = registro.getValue("intereses");
				String stringIntereses = objectIntereses.toString();
				stringIntereses = stringIntereses.replace(" ","");
				stringIntereses = stringIntereses.replace("[","");
				stringIntereses = stringIntereses.replace("]","");
				String[] arrIntereses =  stringIntereses.split(",",0);
				boolean interesaMusicaArte=false;
				for (String interes : arrIntereses){
					if (interes.equals("musica") || interes.equals("arte") ){
						interesaMusicaArte = true;
					}
				}
				if (!interesaMusicaArte) {
					System.out.println("\n" + registro.getValue("nom_usuario") + ", " + registro.getValue("password") + ", " + registro.getValue("genero") + ", " + registro.getValue("fecha_nac") +
							", " + registro.getValue("cont_tweets") + ", " + registro.getValue("intereses") + "\n");
					contadorR++;
				}

			}
		});
		System.out.println("\nSe han encontrado "+contadorR+" registros");
		contadorR=0;


	}
	public void consultaInteresante4(){
		System.out.println("\n**************Mostrando a cuantos usuarios le interesa cada categoria de intereses**************\n");
		System.out.println("Interes			CantUsuarios");
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = true;
		policy.priority = Priority.LOW;
		policy.includeBinData = true;


		cliente.scanAll(policy, "test", "usuarios", new ScanCallback() {
			int contUsuariosMusica;
			int contUsuariosDeportes;
			int contUsuariosLiteratura;
			int contUsuariosTecnologia;
			int contUsuariosFinanzas;
			int contUsuariosArte;
			int contUsuariosFotografia;
			@Override
			public void scanCallback(Key key, Record registro) throws AerospikeException{
				keys.add(registro.getValue("nom_usuario").toString());

				Object objectIntereses = registro.getValue("intereses");
				String stringIntereses = objectIntereses.toString();
				stringIntereses = stringIntereses.replace(" ","");
				stringIntereses = stringIntereses.replace("[","");
				stringIntereses = stringIntereses.replace("]","");
				String[] arrIntereses =  stringIntereses.split(",",0);


					if (Arrays.asList(arrIntereses).contains("musica")){
						contUsuariosMusica=contUsuariosMusica+1;
					}
					if (Arrays.asList(arrIntereses).contains("deportes")){
						contUsuariosDeportes++;
					}
					if (Arrays.asList(arrIntereses).contains("literatura")){
						contUsuariosLiteratura++;
					}
					if (Arrays.asList(arrIntereses).contains("finanzas")){
						contUsuariosFinanzas++;
					}
					if (Arrays.asList(arrIntereses).contains("tecnologia")){
						contUsuariosTecnologia++;
					}
					if (Arrays.asList(arrIntereses).contains("fotografia")){
						contUsuariosFotografia++;
					}
					if (Arrays.asList(arrIntereses).contains("arte")){
						contUsuariosArte++;
					}
					acuArte=acuArte+contUsuariosArte;
					acuMusica=acuMusica+contUsuariosMusica;
					acuDeportes=acuDeportes+contUsuariosDeportes;
					acuFinanzas=acuFinanzas+contUsuariosFinanzas;
					acuFotografia=acuFotografia+contUsuariosFotografia;
					acuTecnologia=acuTecnologia+contUsuariosTecnologia;
					acuLiteratura=acuLiteratura+contUsuariosLiteratura;
					contUsuariosArte=0;
					contUsuariosMusica=0;
					contUsuariosDeportes=0;
					contUsuariosFinanzas=0;
					contUsuariosFotografia=0;
					contUsuariosLiteratura=0;
					contUsuariosTecnologia=0;

			}
		});
		System.out.println("Arte			"+acuArte);
		System.out.println("Musica			"+acuMusica);
		System.out.println("Deportes		"+acuDeportes);
		System.out.println("Finanzas		"+acuFinanzas);
		System.out.println("Fotografia		"+acuFotografia);
		System.out.println("Literatura		"+acuLiteratura);
		System.out.println("Tecnologia		"+acuTecnologia);
	}


	public void consultaInteresante5(){
		System.out.println("\n**************Mostrando al usuario femenino con mayor numero de tweets**************\n");
		System.out.println("nom_usuario, password, genero, fecha_nac, cont_tweets, intereses");
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = true;
		policy.priority = Priority.LOW;
		policy.includeBinData = true;


		cliente.scanAll(policy, "test", "usuarios", new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record registro) throws AerospikeException{
				keys.add(registro.getValue("nom_usuario").toString());
				if (registro.getValue("genero").equals("F")){

					if (Integer.parseInt(registro.getValue("cont_tweets").toString()) > max_tweets){
						max_tweets=Integer.parseInt(registro.getValue("cont_tweets").toString());
						nombreConsultaI5=(registro.getValue("nom_usuario").toString());
						passwordConsultaI5=(registro.getValue("password").toString());
						generoConsultaI5=(registro.getValue("genero").toString());
						fecha_nacConsultaI5=(registro.getValue("fecha_nac").toString());
						cont_tweetsConsultaI5=(registro.getValue("cont_tweets").toString());
						interesesConsultaI5=(registro.getValue("intereses").toString());
					}

				}
			}
		});
		System.out.println("\n" + nombreConsultaI5 + ", " + passwordConsultaI5 + ", " + generoConsultaI5 + ", " + fecha_nacConsultaI5 +
				", " + cont_tweetsConsultaI5 + ", " + interesesConsultaI5 + "\n");
	}
}

