package ua.in.sz.jtaatomikos;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import lombok.extern.slf4j.Slf4j;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

@Slf4j
public class JtaAtomikosApplication {

    public static void main(String[] args)
            throws RollbackException, HeuristicRollbackException, SystemException, HeuristicMixedException {
        UserTransactionImp utx = new UserTransactionImp();
        AtomikosDataSourceBean dataSource = new AtomikosDataSourceBean();
        dataSource.setUniqueResourceName("XA12");
        dataSource.setXaDataSourceClassName("org.postgresql.xa.PGXADataSource");
        Properties p = new Properties();
        p.setProperty("user", "postgres");
        p.setProperty("password", "postgres");
        p.setProperty("serverName", "localhost");
        p.setProperty("portNumber", "5432");
        p.setProperty("databaseName", "postgres");
        dataSource.setXaProperties(p);


        boolean rollback = false;
        try {
            utx.begin();
            Connection inventoryConnection = dataSource.getConnection();


            Statement s1 = inventoryConnection.createStatement();
            String q1 = "update table_name set column_1 = column_1 - 1";
            s1.executeUpdate(q1);
            s1.close();


            inventoryConnection.close();
            log.info("All is fine");
        } catch (Exception e) {
            log.error("Can't run query: ", e);
            rollback = true;
        } finally {
            if (!rollback) {
                utx.commit();
            } else {
                utx.rollback();
            }
        }
    }

}
