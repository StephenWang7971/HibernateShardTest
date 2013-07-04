import java.util.List;
import org.hibernate.shards.ShardId;
import org.hibernate.shards.strategy.selection.ShardSelectionStrategy;
/*
 * a simple ShardSelectionStrategy implementation for our ContactEntity
 */
public class MyShardSelectionStrategy implements ShardSelectionStrategy {
	   private List<ShardId> _shardIds;
	   public MyShardSelectionStrategy(List<ShardId> shardIds){
		   this._shardIds=shardIds;
	   }
	   public ShardId selectShardIdForNewObject(Object obj) {
		   if(obj instanceof ShardableEntity) {
			   String id = ((ShardableEntity)obj).getIdentifier();
               System.out.println("selection param.id="+ id);
			   if(id==null || id.isEmpty()) return this._shardIds.get(0);
			   Integer i = new Integer(id.substring(0, 1));
	    	   //our shard selection is identified by the 
	    	   //first char(number) in contact id
			   //0-4 => shards0, 5-9 => shards1
               System.out.println("on continent:" + this._shardIds.get(i/3));
			   return this._shardIds.get(i/3);
		   }
		   //for non-shardable entities we just use shard0
		   return this._shardIds.get(0);
    }
}
