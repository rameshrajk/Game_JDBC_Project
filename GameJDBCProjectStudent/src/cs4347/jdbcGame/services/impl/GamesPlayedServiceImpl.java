/* NOTICE: All materials provided by this project, and materials derived 
 * from the project, are the property of the University of Texas. 
 * Project materials, or those derived from the materials, cannot be placed 
 * into publicly accessible locations on the web. Project materials cannot 
 * be shared with other project teams. Making project materials publicly 
 * accessible, or sharing with other project teams will result in the 
 * failure of the team responsible and any team that uses the shared materials. 
 * Sharing project materials or using shared materials will also result 
 * in the reporting of all team members for academic dishonesty. 
 */
package cs4347.jdbcGame.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcGame.dao.GamesPlayedDAO;
import cs4347.jdbcGame.dao.impl.GamesPlayedDAOImpl;
import cs4347.jdbcGame.entity.GamesPlayed;
import cs4347.jdbcGame.services.GamesPlayedService;
import cs4347.jdbcGame.util.DAOException;

public class GamesPlayedServiceImpl implements GamesPlayedService
{
    private DataSource dataSource;

    public GamesPlayedServiceImpl(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    @Override	
    public GamesPlayed create(GamesPlayed gamesPlayed) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	GamesPlayed gamesPlayedService = gamesPlayedDAO.create(connection, gamesPlayed);
        	
        	connection.commit();
        	return gamesPlayedService;
        } 
        catch (Exception ex) {
        	connection.rollback();
        	throw ex;
        }
        finally {
        	if (connection != null) {
        		connection.setAutoCommit(true);
        	}
        	if (connection != null && !connection.isClosed()) {
        		connection.close();
        	}
        }
    }

    @Override
    public GamesPlayed retrieveByID(long gamePlayedID) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	GamesPlayed gamesPlayedService = gamesPlayedDAO.retrieveID(connection, gamePlayedID);
        	
        	connection.commit();
        	return gamesPlayedService;
        }
        catch (Exception ex) {
        	connection.rollback();
        	throw ex;
        }
    	finally {
    		if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
    	}
    }

    @Override
    public List<GamesPlayed> retrieveByPlayerGameID(long playerID, long gameID) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	List<GamesPlayed> gamesPlayedList = gamesPlayedDAO.retrieveByPlayerGameID(connection, playerID, gameID);
        	
        	connection.commit();
        	return gamesPlayedList;
        }
        catch (Exception ex){
			connection.rollback();
			throw ex;
		}
		finally{
			if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
		}
    }

    @Override
    public List<GamesPlayed> retrieveByGame(long gameID) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	List<GamesPlayed> gamesPlayedList = gamesPlayedDAO.retrieveByGame(connection, gameID);
        	
        	connection.commit();
        	return gamesPlayedList;
        }
        catch (Exception ex){
			connection.rollback();
			throw ex;
		}
		finally{
			if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
		}
    }

    @Override
    public List<GamesPlayed> retrieveByPlayer(long playerID) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	List<GamesPlayed> gamesPlayedList = gamesPlayedDAO.retrieveByPlayer(connection, playerID);
        	
        	connection.commit();
        	return gamesPlayedList;
        }
        catch (Exception ex){
			connection.rollback();
			throw ex;
		}
		finally{
			if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
		}
    }

    @Override
    public int update(GamesPlayed gamesPlayed) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	int updateRes = gamesPlayedDAO.update(connection, gamesPlayed);
        	
        	connection.commit();
        	return updateRes;
        }
        catch (Exception ex) {
        	connection.rollback();
        	throw ex;
        }
        finally {
        	if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int delete(long gamePlayedID) throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	int deleteRes = gamesPlayedDAO.delete(connection, gamePlayedID);
        	
        	connection.commit();
        	return deleteRes;
        }
        catch (Exception ex) {
        	connection.rollback();
        	throw ex;
        }
        finally {
        	if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int count() throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
    	
        Connection connection = dataSource.getConnection();
        try{
			connection.setAutoCommit(false);
        	int countRes = gamesPlayedDAO.count(connection);
        	
        	connection.commit();
        	return countRes;
		}
		catch (Exception ex){
			connection.rollback();
			throw ex;
		}
		finally{
			if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
		}
    }
}
