import java.security.PrivilegedAction
import java.sql.{DriverPropertyInfo, Connection, Driver}
import java.util.Properties
import java.util.logging.Logger
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.hadoop.security.UserGroupInformation

/**
 * Created by cab on 06/01/2016.
 */
class Krb5SqlServer extends Driver {

  private val sqlServerDriver = new SQLServerDriver()


  override def acceptsURL(url: String): Boolean = sqlServerDriver.acceptsURL(url)

  override def jdbcCompliant(): Boolean = sqlServerDriver.jdbcCompliant()

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = sqlServerDriver.getPropertyInfo(url, info)

  override def getMinorVersion: Int = sqlServerDriver.getMinorVersion

  override def getParentLogger: Logger = sqlServerDriver.getParentLogger

  override def connect(url: String, info: Properties): Connection = {
    val keytabFile = "aaa.keytab"
    var principal = "aaa"

    UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabFile)
      .doAs(new PrivilegedAction[Connection] {
      override def run(): Connection =
        sqlServerDriver.connect(url, info)
    })
  }

  override def getMajorVersion: Int = sqlServerDriver.getMajorVersion
}
