import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name ="tbl_contact")
public class ContactEntity implements ShardableEntity {
    @Id
    @Column(nullable=false)
	private String _id;
    @Column(nullable=false)
	private String _name;
    @Column(nullable=false)
	private String _email;
    @Column(nullable=false)
	private String _loginId;
    @Column(nullable=false)
	private String _password;
	
	public ContactEntity(){}
	public ContactEntity(String id, String loginId, String password
			,String name, String email){
	    this._id = id;
	    this._loginId = loginId;
	    this._password = password;
	    this._name = name;
	    this._email = email;
	}	
	public String getId(){
		return this._id;
	}
	public void setId(String id){
		this._id=id;
	}
	public String getEMail(){
		return this._email;
	}
	public void setEMail(String email){
		this._email=email;
	}
	public String getName(){
		return this._name;
	}
	public void setName(String name){
		this._name=name;
	}
	public String getLoginId(){
		return this._loginId;
	}
	public void setLoginId(String loginId){
		this._loginId=loginId;
	}
	public String getPassword(){
		return this._password;
	}
	public void setPassword(String password){
		this._password=password;
	}
	
	public String getIdentifier(){
		return this._id;
	}
	public String toString(){
		return "{ Id=\"" + this._id + "\""
			+ ", LoginId=\"" + this._loginId + "\""
			+ ", Name=\"" + this._name + "\""
			+ ", EMail=\"" + this._email + "\" }";
	}
}