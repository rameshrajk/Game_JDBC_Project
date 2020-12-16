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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcGame.dao.CreditCardDAO;
import cs4347.jdbcGame.dao.PlayerDAO;
import cs4347.jdbcGame.dao.impl.CreditCardDAOImpl;
import cs4347.jdbcGame.dao.impl.PlayerDAOImpl;
import cs4347.jdbcGame.entity.CreditCard;
import cs4347.jdbcGame.entity.Player;
import cs4347.jdbcGame.services.PlayerService;
import cs4347.jdbcGame.util.DAOException;

public class PlayerServiceImpl implements PlayerService
{
    private DataSource dataSource;

    public PlayerServiceImpl(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    @Override
    public Player create(Player player) throws DAOException, SQLException
    {
        if (player.getCreditCards() == null || player.getCreditCards().size() == 0) {
            throw new DAOException("Player must have at lease one CreditCard");
        }

        PlayerDAO playerDAO = new PlayerDAOImpl();
        CreditCardDAO ccDAO = new CreditCardDAOImpl();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            Player playa = playerDAO.create(connection, player);
            Long playerID = playa.getId();
            for (CreditCard creditCard : player.getCreditCards()) {
                creditCard.setPlayerID(playerID);
                ccDAO.create(connection, creditCard, playerID);
            }
            connection.commit();
            return playa;
        } catch (Exception ex) {
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
    public Player retrieve(Long playerID) throws DAOException, SQLException
    {    	
    	
    	PlayerDAO playerDAO = new PlayerDAOImpl();
        CreditCardDAO ccDAO = new CreditCardDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
      	
        	Player playa = playerDAO.retrieve(connection, playerID);
        	List<CreditCard> creditCards = ccDAO.retrieveCreditCardsForPlayer(connection, playerID);        	        	        	
        	if (creditCards == null || creditCards.size() == 0) {
        		return null;             
            } 
        	playa.setCreditCards(creditCards);

        	connection.commit();
            return playa;
        } catch (Exception ex) {
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
    public int update(Player player) throws DAOException, SQLException
    {
    	PlayerDAO playerDAO = new PlayerDAOImpl();
        CreditCardDAO ccDAO = new CreditCardDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	
        	Long playerID = player.getId();
        	ccDAO.deleteForPlayer(connection, playerID);
        	int updateRes = playerDAO.update(connection, player);
        	
        	List<CreditCard> creditCards = player.getCreditCards(); 
        	for (CreditCard creditCard : creditCards) {
        		if(creditCard.getId() != null)
        			creditCard.setId(null);
                creditCard.setPlayerID(playerID);
                ccDAO.create(connection, creditCard, playerID);
            }
        	
            connection.commit();
			return updateRes;	
        } catch (Exception ex) {
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
    public int delete(Long playerID) throws DAOException, SQLException
    {
    	PlayerDAO playerDAO = new PlayerDAOImpl();
        CreditCardDAO ccDAO = new CreditCardDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try{
			connection.setAutoCommit(false);
        	ccDAO.deleteForPlayer(connection, playerID);  
        	int delRes = playerDAO.delete(connection, playerID);
        	
        	connection.commit();
			return delRes;			
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
    public int count() throws DAOException, SQLException
    {
    	PlayerDAO playerDAO = new PlayerDAOImpl();
    	
        Connection connection = dataSource.getConnection();
        try{
			connection.setAutoCommit(false);
        	int countRes = playerDAO.count(connection);
        	
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

    @Override
    public List<Player> retrieveByJoinDate(Date start, Date end) throws DAOException, SQLException
    {
    	PlayerDAO playerDAO = new PlayerDAOImpl();

		Connection connection = dataSource.getConnection();
		try{
			connection.setAutoCommit(false);
			List <Player> listPlaya = playerDAO.retrieveByJoinDate(connection, start, end);
			connection.commit();
			return listPlaya;
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

    /**
     * Used for debugging and testing purposes.
     */
    @Override
    public int countCreditCardsForPlayer(Long playerID) throws DAOException, SQLException
    {
        CreditCardDAO ccDAO = new CreditCardDAOImpl();

        Connection connection = dataSource.getConnection();
        try{
			connection.setAutoCommit(false);       	      	

        	List<CreditCard> creditCard = ccDAO.retrieveCreditCardsForPlayer(connection, playerID);
        	int countCred = creditCard.size();
        	
        	connection.commit();
        	return countCred;
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
