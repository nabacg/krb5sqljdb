import net.cabworks.jdbc.Krb5SqlServer;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by cab on 06/01/2016.
 */
public class Krb5SqlServerDriver extends Krb5SqlServer {

    // This static block inits the driver when the class is loaded by the JVM.
    static {
        try {
            DriverManager.registerDriver(new Krb5SqlServerDriver());
        } catch (SQLException e) {
            throw new RuntimeException( "Failed to register Krb5SqlServerDriver: " + e.getMessage());
        }
    }
}
