package net.cabworks.jdbc

import org.scalatest.{Matchers, FreeSpec}

/**
 * Created by cab on 12/01/2016.
 */
class Krb5SqlServerTests extends FreeSpec with Matchers{

  "extract correct properties from jdbc url" in {
    val url = "jdbc:sqlserver://serverName:1023;integratedSecurity=true;authenticationScheme=JavaKerberos"

    Krb5SqlServer.connectionProperties(url) should be(Map("integratedSecurity"->"true", "authenticationScheme"->"JavaKerberos"))

  }

  "convert to krb to sql server url" in {
    val principal = "testUser"
    val keytab = "testUser.keytab"
    val url =  "jdbc:sqlserver://serverName:1023;integratedSecurity=true;authenticationScheme=JavaKerberos;"
    val krbUrl = s"jdbc:${Krb5SqlServer.krbPrefix}://serverName:1023;integratedSecurity=true;authenticationScheme=JavaKerberos;${Krb5SqlServer.principalKey}=$principal;${Krb5SqlServer.keytabFile}=$keytab"

    Krb5SqlServer.toSqlServerUrl(krbUrl) should be(url)
  }

  "get principal and keytab path from url" in {
      val principal = "testUser"
      val keytab = "testUser.keytab"

      val krbUrl = s"jdbc:${Krb5SqlServer.krbPrefix}://serverName:1023;${Krb5SqlServer.principalKey}=$principal;${Krb5SqlServer.keytabFile}=$keytab"
      val props = Krb5SqlServer.connectionProperties(krbUrl)

      props.get(Krb5SqlServer.principalKey) should be(Some(principal))
      props.get(Krb5SqlServer.keytabFile) should be(Some(keytab))

  }
}
