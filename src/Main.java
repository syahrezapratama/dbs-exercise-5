import java.io.PrintWriter;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args){

        String dbname = "dvdrental";
        String username = "postgres";
        String password = "130299";

        DvdRentalDB db = new DvdRentalDB(dbname, username, password);

        PrintWriter writer = new PrintWriter(System.out, true);

        // Test getRentalsByCustomer with a known customer name
        System.out.println("Testing getRentalsByCustomer...");
        int rentalsCount = db.getRentalsByCustomer(writer, "Mary Smith");
        System.out.println("Number of rentals: " + rentalsCount);

        // db.closeDBConnection();

    }
}