import java.io.PrintWriter;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args){

        String dbname = "dvdrental";
        String username = "postgres";
        String password = "130299";

        DvdRentalDB db = new DvdRentalDB(dbname, username, password);

        db.closeDBConnection();

    }
}