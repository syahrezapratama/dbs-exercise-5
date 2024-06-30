import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/*
	TODO: HINWEISE - Beachten Sie bei der Umsetzung der Methoden folgende Punkte:
 	- printResult(ResultSet rs, PrintWriter writer) muss für die Ausgabe des Ergebnisses verwendet werden.
 	- DvdRentalUtil.processResults(rs) in printResult(ResultSet rs, PrintWriter writer) darf nicht gelöscht werden.
 		- rs darf vor dem Aufruf von DvdRentalUtil.processResults(rs) nicht geschlossen werden.
 		- Für die erzeugten ResultSets müssen ResultSet.TYPE_SCROLL_INSENSITIVE (resultSetType) und
 			ResultSet.CONCUR_READ_ONLY (resultSetConcurrency) gesetzt sein.
 			(siehe createStatement(int resultSetType, int resultSetConcurrency) bzw.
 			prepareStatement(String sql, int resultSetType, int resultSetConcurrency)

	[NOTE - Comply with the following points when implementing the methods:
		- printResult(ResultSet rs, PrintWriter writer) must be used for outputting the result.
		- DvdRentalUtil.processResults(rs) in printResult(ResultSet rs, PrintWriter writer) must not be deleted.
			- rs must not be closed before calling DvdRentalUtil.processResults(rs).
			- ResultSet.TYPE_SCROLL_INSENSITIVE (resultSetType) and
			ResultSet.CONCUR_READ_ONLY (resultSetConcurrency) must be set for the generated result sets.
			(see createStatement(int resultSetType, int resultSetConcurrency) or
			prepareStatement(String sql, int resultSetType, int resultSetConcurrency)]
 */

public class DvdRentalDB {
	private Connection conn;

	public DvdRentalDB(String dbName, String user, String password) {
		createDBConnection(dbName, user, password);
	}

	/**
	 * Verbindung zum Datenbank-Server aufnehmen
	 * [Connect to the database server]
	 *
	 * @param dbName   Database name
	 * @param user     User name
	 * @param password 4 Punkte
	 */

	private void createDBConnection(String dbName, String user, String password) {
		/* BEGIN */
		/* TODO: HIER muss Code eingefuegt werden */
		try {
			// Class.forName("org.postgresql.Driver").newInstance();
			String url = "jdbc:postgresql://localhost:5432/" + dbName;
			conn = DriverManager.getConnection(url, user, password);
			System.out.println("Connection established successfully.");
			PreparedStatement stmt =
					conn.prepareStatement("SELECT first_name FROM actor WHERE actor_id = ?"); stmt.setInt(1, 1);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (SQLException error) {
			System.out.println("Error:" + error);
		}
		/* END */
	}

	/**
	 * Verbindung zum Datenbank-Server schliessen.
	 * [Close connection to the database server.]
	 * <p>
	 * 2 Punkte
	 */

	public void closeDBConnection() {
		/* BEGIN */
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println("Error closing connection: " + e.getMessage());
			}
		}
		/* END */
	}


	/**
	 * Gibt die Informationen des ResultSets - Spaltennamen und Werte - mittels writer aus
	 * - Zeichenketten sollen in Hochkommata (') stehen
	 * - die einzelnen Spalten sollen durch Tabs separiert werden (\t)
	 * [
	 * Prints the content of the ResultSet - column names and values - via writer
	 * *  - Strings shall be in single quotation marks (')
	 * *  - the individual columns should be separated by tabs (\t)
	 * ]
	 *
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @param rs     ResultSet
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * @throws SQLException 4 Punkte
	 */

	private int printResult(ResultSet rs, PrintWriter writer) throws SQLException {
		int cnt = 0;

		/* BEGIN */
		int columnCount = rs.getMetaData().getColumnCount();

		// Print column names
		for (int i = 1; i <= columnCount; i++) {
			if (i > 1) writer.print("\t");
			writer.print(rs.getMetaData().getColumnName(i));
		}
		writer.println();

		// Print rows
		while (rs.next()) {
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1) writer.print("\t");
				Object obj = rs.getObject(i);
				if (obj instanceof String) {
					writer.print("'" + obj + "'");
				} else {
					writer.print(obj);
				}
			}
			writer.println();
			cnt++;
		}
		/* END */

		// WICHTIG: Hinweise s. oben beachten!
		DvdRentalUtil.processResults(rs);
		return cnt;
	}


	/**
	 * Ausgabe der ausgeliehenen Filmtitel mit Ausleihdatum für einen gegebenen Kunden.
	 * [Display of rented movie titles with rental date for a given customer.]
	 *
	 * @param name   Name des Kunden (Struktur: first_name + ' ' + last_name)
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * <p>
	 * 3 Punkte
	 */

	public int getRentalsByCustomer(PrintWriter writer, String name) {
		int cnt = 0;
		/* BEGIN */
		String sql = "SELECT film.title, rental.rental_date " +
				"FROM customer " +
				"JOIN rental ON customer.customer_id = rental.customer_id " +
				"JOIN inventory ON rental.inventory_id = inventory.inventory_id " +
				"JOIN film ON inventory.film_id = film.film_id " +
				"WHERE CONCAT(customer.first_name, ' ', customer.last_name) = ?";
		try (PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
			stmt.setString(1, name);
			try (ResultSet rs = stmt.executeQuery()) {
				cnt = printResult(rs, writer);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		/* END */
		return cnt;
	}


	/**
	 * Gibt die Namen der Kategorien aus, deren durchschnittliche Leihgebühr (rental_rate)
	 * kleiner als 2.5 ist. Dabei sollen nur Filme, deren Beschreibung 'Shark' beinhaltet,
	 * berücksichtigt werden.
	 * [Displays the names of categories whose average rental rate is less than 2.5, where
	 * only films whose description contains 'Shark' shall be considered.]
	 *
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * <p>
	 * 4 Punkte
	 */

	public int getAffordableCategories(PrintWriter writer) {
		int cnt = 0;
		/* BEGIN */
		String sql = "SELECT category.name " +
				"FROM category " +
				"JOIN film_category ON category.category_id = film_category.category_id " +
				"JOIN film ON film_category.film_id = film.film_id " +
				"WHERE film.description LIKE '%Shark%' " +
				"GROUP BY category.name " +
				"HAVING AVG(film.rental_rate) < 2.5";
		try (PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = pstmt.executeQuery()) {
			cnt = printResult(rs, writer);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		/* END */
		return cnt;
	}


	/**
	 * Für den Film mit dem neuesten Erscheinungsdatum einer* gegebenen Schauspieler*in
	 * sollen alle Ausleihen ermittelt werden, bei denen dieser Film
	 * länger als 6 (volle) Tage ausgeliehen wurde.
	 * Für jede solche Ausleihe soll die ID der Ausleihe (rental_id), das Ausleihdatum und das
	 * Rückgabedatum auf writer ausgegeben werden.
	 * Falls mehrere Filme der gegebenen Schauspieler:in im selben Jahr erschienen sind,
	 * verwenden Sie aus der Menge der Filme mit neustem Erscheinungsjahr denjenigen,
	 * welcher die minimale ID besitzt.
	 * <p>
	 * [For the film with the most recent release year of a given actor, all rentals should be determined
	 * where this film was rented for longer than 6 (full) days.
	 * For each such rental, the rental id, the rental date and the return date should be output
	 * to writer. If several films of the given actor were released in the same year,
	 * use the one with the smallest ID from the set of films with the most recent release year.]
	 *
	 * @param actor  Name der* Schauspieler*in (Struktur: first_name + ' ' + last_name)
	 * @param writer PrinterWriter, auf den das Ergebnis geschrieben werden soll
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * 5 Punkte
	 */

	public int latestMovieRented(PrintWriter writer, String actor) {
		int cnt = 0;
		/* BEGIN */
		String sql = "WITH latest_film AS (" +
				"  SELECT film.film_id, film.title, film.release_year " +
				"  FROM film " +
				"  JOIN film_actor ON film.film_id = film_actor.film_id " +
				"  JOIN actor ON film_actor.actor_id = actor.actor_id " +
				"  WHERE CONCAT(actor.first_name, ' ', actor.last_name) = ? " +
				"  ORDER BY film.release_year DESC, film.film_id ASC " +
				"  LIMIT 1" +
				") " +
				"SELECT rental.rental_id, rental.rental_date, rental.return_date " +
				"FROM rental " +
				"JOIN inventory ON rental.inventory_id = inventory.inventory_id " +
				"JOIN latest_film ON inventory.film_id = latest_film.film_id " +
				"WHERE rental.return_date > rental.rental_date + INTERVAL '6 days'";
		try (PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
			pstmt.setString(1, actor);
			try (ResultSet rs = pstmt.executeQuery()) {
				cnt = printResult(rs, writer);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		/* END */
		return cnt;
	}

	/**
	 * Gibt den Kunden (customer_id) aus, der denselben Film am häufigsten wiederholt ausgeliehen hat,
	 * sowie den Namen des entsprechenden Films.
	 * Wenn es mehrere Kunden gibt, für die die Zahl der wiederholten Ausleihen gleich ist,
	 * dann soll der mit der kleinsten customer_id ausgegeben werden.
	 * Beispiel: Hat Max mit customer_id = 1 den Film 'Up' 10x ausgeliehen und alle anderen Kunden haben
	 * noch nie einen (beliebigen) Film mehr als 2x ausgeliehen, dann wird (1, 'Up') ausgegeben.
	 * <p>
	 * [Displays the customer (customer_id) who has rented the same film the most repeatedly
	 * together with the name of the corresponding film. If there are several customers who have rented a film
	 * the same number of times, then the one with the smallest customer_id should be returned.
	 * Example: If Max with customer_id = 1 has rented the film 'Up' 10 times and
	 * all other customers have never rented any film more than 2 times, then the output is (1, 'Up').]
	 *
	 * @param writer
	 * @return int Anzahl der selektierten Tupel des Ergebnisses
	 * 5 Punkte
	 */

	public int getDieHardFan(PrintWriter writer) {
		int cnt = 0;
		/* BEGIN */
		String sql = "SELECT customer_id, title " +
				"FROM (" +
				"  SELECT customer.customer_id, film.title, COUNT(*) AS rental_count, " +
				"         RANK() OVER (ORDER BY COUNT(*) DESC, customer.customer_id ASC) AS rnk " +
				"  FROM rental " +
				"  JOIN inventory ON rental.inventory_id = inventory.inventory_id " +
				"  JOIN film ON inventory.film_id = film.film_id " +
				"  JOIN customer ON rental.customer_id = customer.customer_id " +
				"  GROUP BY customer.customer_id, film.title" +
				") AS ranked_rentals " +
				"WHERE rnk = 1 " +
				"ORDER BY customer_id ASC " +
				"LIMIT 1";
		try (PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			 ResultSet rs = pstmt.executeQuery()) {
			cnt = printResult(rs, writer);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		/* END */
		return cnt;

	}

	/**
	 * Aktualisiert die E-Mail-Adresse eines Kunden
	 * <p>
	 * [Updates a customer's email address]
	 *
	 * @param customerID ID des Kunden
	 * @param email      Neue E-Mail-Adresse
	 *                   <p>
	 *                   4 Punkte
	 */

	public void updateEmailAddress(int customerID, String email) {
		/* BEGIN */
		String sql = "UPDATE customer SET email = ? WHERE customer_id = ?";
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, email);
			pstmt.setInt(2, customerID);
			pstmt.executeUpdate();
			System.out.println("Email address updated successfully for customer ID: " + customerID);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Error updating email address for customer ID: " + customerID);
		}
		/* END */
	}


	/**
	 * Loescht einen Film aus der DB
	 * <p>
	 * [Deletes a film from the DB]
	 *
	 * @param filmID ID des zu loeschenden Films
	 *               <p>
	 *               3 Punkte
	 */

	public void deleteFilm(int filmID) {
		/* BEGIN */
		String sql = "DELETE FROM film WHERE film_id = ?";
		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setInt(1, filmID);
			int rowsAffected = pstmt.executeUpdate();
			if (rowsAffected > 0) {
				System.out.println("Film with ID " + filmID + " deleted successfully.");
			} else {
				System.out.println("No film found with ID " + filmID);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Error deleting film with ID: " + filmID);
		}
		/* END */
	}
}