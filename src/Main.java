import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.shards.*;
import org.hibernate.shards.cfg.*;
import org.hibernate.shards.strategy.*;
import org.hibernate.shards.strategy.access.*;
import org.hibernate.shards.strategy.resolution.*;
import org.hibernate.shards.strategy.selection.*;
import org.hibernate.shards.session.ShardedSessionFactory;

public class Main {
	public static void main(String[] args) {
		HibernateShardsTest(args);
	}
	private static SessionFactory createSessionFactory() {
        Configuration prototypeCfg = new Configuration()
            .configure("shard0.hibernate.cfg.xml");
        List<ShardConfiguration> shardCfgs = new ArrayList<ShardConfiguration>();
        shardCfgs.add(buildShardConfig("shard0.hibernate.cfg.xml"));
        shardCfgs.add(buildShardConfig("shard1.hibernate.cfg.xml"));
        shardCfgs.add(buildShardConfig("shard2.hibernate.cfg.xml"));
        ShardStrategyFactory strategyFactory = buildShardStrategyFactory();
        ShardedConfiguration shardedConfig = new ShardedConfiguration(
            prototypeCfg, shardCfgs, strategyFactory);
       return shardedConfig.buildShardedSessionFactory();
   }
    private static ShardStrategyFactory buildShardStrategyFactory() {
       return new ShardStrategyFactory() {
           public ShardStrategy newShardStrategy(List<ShardId> shardIds) {
               ShardSelectionStrategy ss = new MyShardSelectionStrategy(shardIds);
               ShardResolutionStrategy rs = new MyShardResolutionStrategy(shardIds);
               ShardAccessStrategy as = new SequentialShardAccessStrategy();
               return new ShardStrategyImpl(ss, rs, as);
           }
       };
    }
    private static ShardConfiguration buildShardConfig(String configFile) {
        Configuration config = new Configuration().configure(configFile);
        return new ConfigurationToShardConfigurationAdapter(config);
    }
	private static void HibernateShardsTest(String[] args){
		String loginId = "RicCC@cnblogs.com";
		String password = "123";
		if(args!=null && args.length==2){
			loginId = args[0];
			password = args[1];
		}
		SessionFactory factory = null;
		try{
			factory = createSessionFactory();
			ShardsTestCreate(factory);
			ShardsTestLogin(factory, loginId, password);
			//ShardsTestDelete(factory);
		}catch(Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		}finally{
			if(factory!=null) factory.close();
		}
	}
	private static void ShardsTestCreate(SessionFactory factory){
		Session session = null;
		Transaction transaction = null;
		System.out.println("===Create Contacts===");
		try{
			session = factory.openSession();
			transaction = session.beginTransaction();
			session.save(new ContactEntity("01111111","RicCC@cnblogs.com"
					, "123", "Richie", "RicCC@cnblogs.com"));
            transaction.commit();
            transaction = session.beginTransaction();
			session.save(new ContactEntity("21111111","a@cnblogs.com"
					, "123", "AAA", "a@cnblogs.com"));
            transaction.commit();
            transaction = session.beginTransaction();
			session.save(new ContactEntity("81111111","b@cnblogs.com"
					, "123", "BBB", "b@cnblogs.com"));
            transaction.commit();
            transaction = session.beginTransaction();
			session.save(new ContactEntity("31111111","c@cnblogs.com"
					, "123", "CCC", "c@cnblogs.com"));
			transaction.commit();
		}catch(Exception e){
			if(transaction!=null) transaction.rollback();
			System.out.println(e.getMessage());
			e.printStackTrace();
		}finally{
			if(session!=null) session.close();
		}
	}
	private static void ShardsTestLogin(SessionFactory factory
			, String loginId, String password){
		Session session = null;
		ContactEntity c = null;
		System.out.println("\n===Login Test===");
		try{
			session = factory.openSession();
			List contacts = session.createQuery("from ContactEntity where LoginId = :loginId")
			    .setString("loginId", loginId)
			    .list();
			if(contacts.isEmpty())
				System.out.println("Contact \"" + loginId + "\" not found!");
			else{
			    c = (ContactEntity)contacts.get(0);
			    if(c.getPassword().equals(password))
			    	System.out.println("Contact \"" + loginId + "\" login successful");
			    else
			    	System.out.println("Password is incorrect (should be: "
			    	    + c.getPassword() + ", but is: " + password + ")");
			}

			System.out.println("\n===Get Contact by Id===");
			c = (ContactEntity)session.get(ContactEntity.class, "81111111");
			System.out.println(c.toString());
			c = (ContactEntity)session.get(ContactEntity.class, "31111111");
			System.out.println(c.toString());
            c = (ContactEntity)session.get(ContactEntity.class, "21111111");
            System.out.println(c.toString());
            c = (ContactEntity)session.get(ContactEntity.class, "01111111");
            System.out.println(c.toString());
		}catch(Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		}finally{
			if(session!=null) session.close();
		}
	}

    /*
	private static void ShardsTestDelete(SessionFactory factory){
		Session session = null;
		Transaction transaction = null;
		System.out.println("\n===Delete Contacts===");
		try{
			session = factory.openSession();
			transaction = session.beginTransaction();
			List contacts = session.createQuery("from ContactEntity").list();
			Iterator it = contacts.iterator();
			while(it.hasNext()){
				session.delete(it.next());
			}
			transaction.commit();
		}catch(Exception e){
			if(transaction!=null) transaction.rollback();
			System.out.println(e.getMessage());
			e.printStackTrace();
		}finally{
			if(session!=null) session.close();
		}
	}
		*/
}