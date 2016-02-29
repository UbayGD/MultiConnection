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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Esta clase permite la conexión a diferentes tipos de BD mediante métodos sencillos y unificados.
 * @author ubay
 */
public class MultiConnection implements Serializable{
    
    private int mode = -1;
    final static int MY_SQL = 0;
    final static int POSTGRES_SQL = 1;
    final static int MS_SQLSERVER = 2;
    private Connection conexion;
    //final static int ORACLE = 2;
    //final static int ANY_SQL = 3;
    //final static int OTHER_SQL = 4;
    
    /**
     * Constructor por defecto.
     */
    public MultiConnection() {
    }

    /**
     * Constructor donde se inicializa el modo.
     * @param mode 
     */
    public MultiConnection(int mode) {
        this.mode = mode;
    }
    
    /**
     * Constructor donde se inicializa la conexion.
     * @param conexion 
     */
    public MultiConnection(Connection conexion) {
        this.conexion = conexion;
    }

    /**
     * Constructor donde se inicializa el modo y la conexion.
     * @param mode
     * @param conexion 
     */
    public MultiConnection(int mode, Connection conexion) {
        this.mode = mode;
        this.conexion = conexion;
    }

    /**
     * Establece el modo para definir la base de datos empleada. <br>
     * 0 - MySQL <br> 
     * 1 - PosgreSQL <br>
     * 2 - MS SQL Server
     * @param mode 
     */
    public void setMode(int mode) {
        this.mode = mode;
    }

    /**
     * Obtiene el modo actual
     * @return 
     */
    public int getMode() {
        return mode;
    }

    /**
     * Obtiene el objeto conexion
     * @return 
     */
    public Connection getConexion() {
        return conexion;
    }

    /**
     * Inicializa el objeto conexion.
     * @param conexion 
     */
    public void setConexion(Connection conexion) {
        this.conexion = conexion;
    }
    
    /**
     * Devuelve un objeto con la conexión. Según el modo establecido, obtendremos
     * la conexión del SGBD elegido.
     * @param database
     * @param user
     * @param passwd
     * @param ip
     * @param port
     * @return 
     */
    public Object connect(String database, String user, String passwd, String ip, String port ) {
        
        switch (mode) { 
            case MY_SQL:
                try {
                    conexion = new MySQL(database, user, passwd, ip, port).getCnx();
                } catch (Exception ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case POSTGRES_SQL:
                try {
                    conexion = new PostgreSQL(database, user, passwd, ip, port).getConexion();
                } catch (Exception ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case MS_SQLSERVER:
                try  {
                    conexion = new SQLServer(database, user, passwd, ip, port).getCnx();
                }
                catch (Exception ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
        }
        return conexion;
    }
    
    /**
     * Realiza la desconexión con la base de datos
     */
    public void disconnect() {
    
        if (conexion != null) {
            switch (mode) { 
                case MY_SQL:
                    try {
                        new MySQL().closeConnection(conexion);
                    } catch (SQLException ex) {
                        Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    break;
                case POSTGRES_SQL:
                    try {
                        new PostgreSQL().closeConnection(conexion);
                    } catch (SQLException ex) {
                        Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    break;
                case MS_SQLSERVER:
                    try {
                        new SQLServer().closeConnection(conexion);
                    } catch (SQLException ex) {
                        Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    break;
            }
        }  
    }
    
    /**
     * Ejecuta una consulta y devuelve un ArrayList que a su vez contendrá array
     * de objetos.
     * @param query
     * @return 
     */
    public ArrayList<Object[]> executeQuery(String query) {
        
        ArrayList<Object[]> consulta = null;
        
        switch (mode) { 
            case MY_SQL:
                try {
                    consulta = new MySQL().query(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case POSTGRES_SQL:
                try {
                    consulta = new PostgreSQL().query(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case MS_SQLSERVER:
                try {
                    consulta = new SQLServer().query(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
        }
        
        return consulta;
    }
    
    /**
     * Ejecuta una consulta de tipo Insert.
     * @param query 
     */
    public void executeInsert(String query) {
    
        switch (mode) { 
            case MY_SQL:
                try {
                    new MySQL().insert(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case POSTGRES_SQL:
                try {
                    new PostgreSQL().insert(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case MS_SQLSERVER:
                try {
                    new SQLServer().insert(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
        }
    }
    
    /**
     * Ejecuta una consulta de tipo Delete.
     * @param query 
     */
    public void executeUpdate(String query) {
    
        switch (mode) { 
            case MY_SQL:
                try {
                    new MySQL().update(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case POSTGRES_SQL:
                try {
                    new PostgreSQL().update(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case MS_SQLSERVER:
                try {
                    new SQLServer().update(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
        }
    }
    
    /**
     * Ejecuta una consulta de tipo Delete.
     * @param query 
     */
    public void executeDelete(String query) {
    
        switch (mode) { 
            case MY_SQL:
                try {
                    new MySQL().delete(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case POSTGRES_SQL:
                try {
                    new PostgreSQL().delete(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
            case MS_SQLSERVER:
                try {
                    new SQLServer().delete(conexion, query);
                } catch (SQLException ex) {
                    Logger.getLogger(MultiConnection.class.getName()).log(Level.SEVERE, null, ex);
                }
                break;
        }
    }
}