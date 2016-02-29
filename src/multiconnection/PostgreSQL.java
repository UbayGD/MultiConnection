/*
 * Copyright (C) 2016 ubay
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package multiconnection;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Esta clase se encarga de las operaciones con PostgreSQL.
 * @author ubay
 */
public class PostgreSQL implements Serializable{
    
    private String database;
    private String user;
    private String passwd;
    private String ip;
    private String port;
    private Connection cnx = null;

    /**
     * Constructor por defecto
     */
    protected PostgreSQL() {
    }

    /**
     * Constructor con todo los parametros
     * @param database
     * @param user
     * @param passwd
     * @param ip
     * @param port 
     */
    protected PostgreSQL(String database, String user, String passwd, String ip, String port) {
        this.database = database;
        this.user = user;
        this.passwd = passwd;
        this.ip = ip;
        this.port = port;
    }
    
    /**
     * Constructor de copia
     * @param psql 
     */
    protected PostgreSQL(PostgreSQL psql) {
        this.database = psql.getDatabase();
        this.user = psql.getUser();
        this.passwd = psql.getPasswd();
        this.ip = psql.getIp();
        this.port = psql.getPort();
    }
    
    protected String getDatabase() {
        return database;
    }

    protected void setDatabase(String database) {
        this.database = database;
    }

    protected String getUser() {
        return user;
    }

    protected void setUser(String user) {
        this.user = user;
    }

    protected String getPasswd() {
        return passwd;
    }

    protected void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    protected String getIp() {
        return ip;
    }

    protected void setIp(String ip) {
        this.ip = ip;
    }

    protected String getPort() {
        return port;
    }

    protected void setPort(String port) {
        this.port = port;
    }

    /**
     * Obtiene la conexion PostgreSQL.
     * @return 
     * @throws java.lang.Exception 
     */
    protected Connection getConexion() throws Exception{
        
        cnx = null;

        Class.forName("org.postgresql.Driver");
        cnx = DriverManager.getConnection("jdbc:postgresql://" +
                    ip + ":" + port + "/" + database, user, passwd);
        return cnx;
    }

    protected void setCnx(Connection cnx) {
        this.cnx = cnx;
    }
    
    /**
     * Cierra la conexion con la BD.
     * @param conexion
     * @throws SQLException 
     */
    protected void closeConnection(Connection conexion) throws SQLException{
        conexion.close();
    }
    
    /**
     * Ejecuta la consulta que recibe como parametro en forma de String.
     * @param cnx
     * @param query
     * @return Devuelve un ArrayList de Object[]
     * @throws SQLException 
     */
    protected ArrayList<Object[]> query(Connection cnx, String query) throws SQLException {
    
        ArrayList<Object[]> lista = new ArrayList<>();
        
        query = new QueryClear().doIt(query);
        
        PreparedStatement st = cnx.prepareStatement(query);
        ResultSet rs = st.executeQuery();
            
        Object[] fila = null;
            
        ResultSetMetaData metaData = rs.getMetaData();
        int n = metaData.getColumnCount();
            
        while (rs.next()) {
            fila = new Object[n];

            for (int i = 1; i <= n; i++) {
                fila[i - 1] = rs.getObject(i);
            }
            lista.add(fila);
        }

        return lista;
    }
    
    /**
     * Realiza una llamada al metodo action(), pasandole el parametro query.
     * @param cnx
     * @param query
     * @throws SQLException 
     */
    protected void insert(Connection cnx, String query) throws SQLException {
        query = new QueryClear().doIt(query);
        action(cnx, query);
    }
    
    /**
     * Realiza una llamada al metodo action(), pasandole el parametro query.
     * @param cnx
     * @param query
     * @throws SQLException 
     */
    protected void delete(Connection cnx, String query) throws SQLException {
        query = new QueryClear().doIt(query);
        action(cnx, query);
    }
    
    /**
     * Realiza una llamada al metodo action(), pasandole el parametro query.
     * @param cnx
     * @param query
     * @throws SQLException 
     */
    protected void update(Connection cnx, String query) throws SQLException {
        query = new QueryClear().doIt(query);
        action(cnx, query);
    }
    
    /**
     * Metodo que ejecuta los insert, update y delete.
     * @param cnx
     * @param query
     * @throws SQLException 
     */
    private void action(Connection cnx, String query) throws SQLException {
        Statement st = cnx.createStatement();
        st.executeUpdate(query);
        st.close();
    }
}
